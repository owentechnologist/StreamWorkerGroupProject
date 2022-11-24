package com.redislabs.sa.ot.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.*;

import static java.util.Collections.singletonMap;

public class RedisStreamAdapter {

    private JedisPool connectionPool;
    private String streamName;
    private String consumerGroupName;
    private int oneDay = 60*60*24*1000;
    private long printcounter = 0;


    public RedisStreamAdapter(String streamName, JedisPool connectionPool){
        this.connectionPool=connectionPool;
        this.streamName=streamName;
    }

    // this classes' constructor determines the target StreamName
    // we need to only provide the consumer group name
    public void createConsumerGroup(String consumerGroupName){
        this.consumerGroupName = consumerGroupName;
        StreamEntryID nextID = StreamEntryID.LAST_ENTRY; //This is the point at which the group begins
        try {
            String thing = this.connectionPool.getResource().xgroupCreate(this.streamName, this.consumerGroupName, nextID, true);
            System.out.println(this.getClass().getName()+" : Result returned when creating a new ConsumerGroup "+thing);
        }catch(JedisDataException jde){
            if(jde.getMessage().contains("BUSYGROUP")) {
                System.out.println("ConsumerGroup " + consumerGroupName + " already exists -- continuing");
            }else {
                jde.printStackTrace();
            }
        }
    }

    // This Method can be invoked multiple times each time with a unique consumerName
    //Assumes The group has been created - now we want a single named consumer to start
    // using 0 will grab any pending messages for that listener in case it failed mid-processing
    public void namedGroupConsumerStartListening(String consumerName, StreamEventMapProcessor streamEventMapProcessor){
        new Thread(new Runnable() {
            @Override
            public void run() {
                String key = "0"; // get all data for this consumer in case it is in recovery mode
                List<StreamEntry> streamEntryList = null;
                StreamEntry value = null;
                StreamEntryID lastSeenID = null;
                System.out.println("RedisStreamAdapter.namedGroupConsumerStartListening(--> "+consumerName+"  <--): Actively Listening to Stream "+streamName);
                long counter = 0;
                Map.Entry<String, StreamEntryID> streamQuery = null;
                while(true) {
                    try (Jedis streamReader = connectionPool.getResource();) {
                        //grab one entry from the target stream at a time
                        //block for long time between attempts
                        XReadGroupParams xReadGroupParams = new XReadGroupParams().block(oneDay).count(1);
                        HashMap hashMap = new HashMap();
                        hashMap.put(streamName,StreamEntryID.UNRECEIVED_ENTRY);
                        List<Map.Entry<String, List<StreamEntry>>> streamResult =
                                streamReader.xreadGroup(consumerGroupName,consumerName,
                                        xReadGroupParams,
                                        (Map<String, StreamEntryID>)hashMap);
                        key = streamResult.get(0).getKey(); // name of Stream
                        streamEntryList = streamResult.get(0).getValue(); // we assume simple use of stream with a single update
                        value = streamEntryList.get(0);// entry written to stream
                        //only print 1 of 10 these next lines:
                        if(printcounter%10==0) {
                            System.out.println("Consumer " + consumerName + " of ConsumerGroup " + consumerGroupName + " has received... " + key + " " + value);
                        }
                        printcounter++;
                        Map<String, StreamEntry> entry = new HashMap();
                        entry.put(key+":"+value.getID()+":"+consumerName,value);
                        lastSeenID = value.getID();
                        streamEventMapProcessor.processStreamEventMap(entry);
                        streamReader.xack(key, consumerGroupName, lastSeenID);
                        streamReader.xdel(key,lastSeenID);// delete test
                    }
                }
            }
        }).start();
    }

    // this does not use consumer groups - all messages are consumed from target stream
    // This would not be the best choice if the desire is to have recoverable events
    public void listenToStream(StreamEventMapProcessor streamEventMapProcessor){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try (Jedis streamListener =  connectionPool.getResource();){
                        String key = "";
                        List<StreamEntry> streamEntryList = null;
                        StreamEntry value = null;
                        StreamEntryID nextID = new StreamEntryID();
                        System.out.println("main.kickOffStreamListenerThread: Actively Listening to Stream "+streamName);
                        Map<String, StreamEntryID> streamQuery = singletonMap(streamName, nextID);
                        XReadParams xReadParams = new XReadParams().block(oneDay).count(1);
                        while(true){
                            streamQuery = singletonMap(streamName, nextID);
                            List<Map.Entry<String, List<StreamEntry>>> streamResult =
                                    streamListener.xread(xReadParams,streamQuery);
                            key = streamResult.get(0).getKey(); // name of Stream
                            streamEntryList = streamResult.get(0).getValue(); // we assume simple use of stream with a single update

                            value = streamEntryList.get(0);// entry written to stream
                            System.out.println("[Not part of a ConsumerGroup] StreamListenerThread: received... "+key+" "+value);
                            Map<String,StreamEntry> entry = new HashMap();
                            entry.put(key+":"+value.getID(),value);
                            streamEventMapProcessor.processStreamEventMap(entry);
                            nextID = value.getID();
                        }
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }
            }).start();
    }

}
