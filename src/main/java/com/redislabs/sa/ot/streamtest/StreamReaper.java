package com.redislabs.sa.ot.streamtest;

import com.github.javafaker.Faker;
import redis.clients.jedis.*;
import redis.clients.jedis.params.XClaimParams;
import redis.clients.jedis.params.XTrimParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                    try{
                        Thread.sleep(pendingMessageTimeout*300); // default is 100 millis *300 or 30 seconds
                    }catch(InterruptedException ie){}
                    try (Jedis streamReader = connectionPool.getResource();) {
                        System.out.println(this.getClass().getCanonicalName()+" -- Claiming and trimming...  attempt # "+counter);
                        //trim output stream of any events older than X hours
                        XTrimParams xTrimParams = new XTrimParams().maxLen(100);//.approximateTrimming().minId(""+(System.currentTimeMillis()-HISTORY_TIMEOUT_LENGTH_MILLIS)+"-0");
                        streamReader.xtrim(StreamConstants.OUTPUT_STREAM_NAME,xTrimParams);
                        try {
                            StreamPendingSummary streamPendingSummary = streamReader.xpending(streamName, StreamConstants.CONSUMER_GROUP_NAME);
                            System.out.println("We have this many PENDING Entries: " + streamPendingSummary.getTotal());
                            if (streamPendingSummary.getTotal() > 0) {
                                String consumerID = (String) streamPendingSummary.getConsumerMessageCount().keySet().toArray()[0];
                                System.out.println("Min ID of the PENDING entries equals: " + streamPendingSummary.getMinId());
                                System.out.println("consumerID == " + consumerID);
                                List<StreamEntry> streamEntries = streamReader.xclaim(streamName, groupName, consumerID, 30, 0,
                                        0, true, streamPendingSummary.getMinId());
                                if (streamEntries.size() > 0) {
                                    System.out.println("We got a live one: " + streamEntries.get(0).getID());
                                    StreamEntry value = streamEntries.get(0);
                                    HashMap<String, String> poisonPayload = new HashMap<>((Map) value.getFields());
                                    poisonPayload.put("id", value.getID().toString());
                                    new StreamEventMapProcessorToHash().processPoisonPill((Map) poisonPayload);
                                    streamReader.xack(streamName, groupName, streamEntries.get(0).getID());
                                }
                            }
                        }catch (Throwable t){System.out.println("Reaper Looking for Poison...> "+t.getMessage());}
                        counter++;
                        counter=counter%15; // reset counter to zero every 15 tries
                        }
                    }
                }
        }).start();
    }

}
