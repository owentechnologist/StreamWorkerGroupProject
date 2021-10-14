package com.redislabs.sa.ot.streamtest;

import com.github.javafaker.Faker;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

import static com.redislabs.sa.ot.streamtest.StreamConstants.PAYLOAD_KEYNAME;

public class StreamWriter {

    private JedisPool connectionPool;
    private String streamName;
    private static Faker faker = new Faker();

    public StreamWriter(String streamName, JedisPool connectionPool){
        this.connectionPool=connectionPool;
        this.streamName=streamName;
    }

    public void kickOffStreamEvents(long maxNumber, long pauseSize){
        new Thread(new Runnable() {
            @Override
            public void run() {
                Map<String, String> map1 = new HashMap<>();
                long counter = 1;
                while (true) {
                    try (Jedis streamReader = connectionPool.getResource();) {
                        String payload = faker.name().firstName();
                        map1.put(PAYLOAD_KEYNAME, payload);
                        streamReader.xadd(streamName, null, map1);
                        counter++;
                        if(counter % pauseSize ==0){
                            try{
                                Thread.sleep(30);
                            }catch(InterruptedException ie){}
                        }
                        if(counter>maxNumber){
                            break;
                        }
                    }
                }
            }
        }).start();
    }
}
