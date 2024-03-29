package com.redislabs.sa.ot.streamtest;

import com.github.javafaker.Faker;
import redis.clients.jedis.*;
import redis.clients.jedis.params.XClaimParams;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.params.XTrimParams;
import redis.clients.jedis.resps.StreamPendingSummary;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class StreamReaper {

    private JedisPool connectionPool;
    private String streamName;
    private static Faker faker = new Faker();

    public StreamReaper(String streamName, JedisPool connectionPool){
        this.connectionPool=connectionPool;
        this.streamName=streamName;
    }

    /*
    This method does two things every 30 seconds:
    1) Executes a call to Xtrim the OUTPUT_STREAM to the most recent 100 entries (assuming one is in use)
    2) looks for poisonpill Stream Events that are in pending state and sends them for processing/cleanup
    (you can find the resulting Hash record of the poisonpill by scanning for H:ProcessedEvent:::*
     */
    public void kickOffStreamReaping(long pendingMessageTimeout,String groupName){
        new Thread(new Runnable() {
            @Override
            public void run() {
                Map<String, String> map1 = new HashMap<>();
                long counter = 0;
                while (true) {
                    try{ //PENDING_MESSAGE_TIMEOUT originates from Class StreamConstants and in this version is not modified by caller 'Main'
                        Thread.sleep(pendingMessageTimeout); // default is 60 seconds
                    }catch(InterruptedException ie){}
                    try (Jedis streamReader = connectionPool.getResource();) {
                        System.out.println(this.getClass().getName()+" -- Claiming and trimming loop...  attempt # "+counter+"   "+new Date());
                        //trim output stream of any events other than the most recent 100 events
                        XTrimParams xTrimParams = new XTrimParams().maxLen(100);
                        streamReader.xtrim(StreamConstants.OUTPUT_STREAM_NAME,xTrimParams);
                        try {
                            StreamPendingSummary streamPendingSummary = streamReader.xpending(streamName, StreamConstants.CONSUMER_GROUP_NAME);
                            System.out.println("We have this many PENDING Entries: " + streamPendingSummary.getTotal());
                            if (streamPendingSummary.getTotal() > 0) {
                                String consumerID = (String) streamPendingSummary.getConsumerMessageCount().keySet().toArray()[0];
                                System.out.println("Min ID of the PENDING entries equals: " + streamPendingSummary.getMinId());
                                System.out.println("consumerID == " + consumerID);

                                List<StreamEntry> streamEntries = streamReader.xclaim(streamName,groupName,consumerID,30, XClaimParams.xClaimParams(),streamPendingSummary.getMinId());

                                if (streamEntries.size() > 0) {
                                    System.out.println("We got a live one: " + streamEntries.get(0).getID());
                                    StreamEntry discoveredPendingStreamEntry = streamEntries.get(0);
                                    if(((String)discoveredPendingStreamEntry.getFields()
                                            .get(StreamConstants.PAYLOAD_KEYNAME)).equalsIgnoreCase("poisonpill")) {
                                        Map<String,StreamEntry> poisonPayload = new HashMap();
                                        poisonPayload.put(streamName+":"+discoveredPendingStreamEntry.getID()+":"+consumerID,discoveredPendingStreamEntry);
                                        new StreamEventMapProcessorToHash().processPoisonPill((Map) poisonPayload);
                                    }else{  // we have a normal Pending message - just send it for regular processing
                                        Map<String,StreamEntry> entry = new HashMap();
                                        entry.put(streamName+":"+discoveredPendingStreamEntry.getID()+":"+consumerID,discoveredPendingStreamEntry);
                                        new StreamEventMapProcessorToHash().processStreamEventMap(entry);
                                    }
                                    streamReader.xack(streamName, groupName, discoveredPendingStreamEntry.getID());
                                    streamReader.xdel(streamName, discoveredPendingStreamEntry.getID());
                                }
                            }
                        }catch (Throwable t){
                            if(null == t.getMessage()){
                                System.out.println(this.getClass().getName()+" : There are no pending messages to clean up");
                                t.printStackTrace();
                            }else {
                                if (t.getMessage().equalsIgnoreCase("Cannot read the array length because \"bytes\" is null")) {
                                    //do nothing
                                    //System.out.println("Reaper Looking for Poison...> None Found this time which results in null: "+t.getMessage());
                                } else {
                                    t.printStackTrace();
                                }
                            }
                        }
                        counter++;
                        counter=counter%10; // reset counter to zero every 10 tries
                        if(counter==0) {
                            System.out.println("\t>>> Resetting StreamReaper Counter to 0 at " + new Date());
                        }
                    }
                }
            }
        }).start();
    }

}
