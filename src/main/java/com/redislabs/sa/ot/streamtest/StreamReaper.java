package com.redislabs.sa.ot.streamtest;

import com.github.javafaker.Faker;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAutoClaimParams;

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
                        streamReader.xautoclaim(streamName, groupName,
                                "1", pendingMessageTimeout, new StreamEntryID("0-0"),
                                XAutoClaimParams.xAutoClaimParams().xAutoClaimParams().count(2));
                        counter++;
                        }
                    }
                }
        }).start();
    }

}
