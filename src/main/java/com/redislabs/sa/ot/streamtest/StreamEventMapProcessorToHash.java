package com.redislabs.sa.ot.streamtest;

import com.redislabs.sa.ot.util.JedisConnectionFactory;
import com.redislabs.sa.ot.util.StreamEventMapProcessor;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.redislabs.sa.ot.streamtest.StreamConstants.*;

/**
 * This class processes Event Entries passed to it by the RedisStreamAdapter
 * It outputs Redis Hash objects that are written to Redis as evidence of the
 * work being completed
 * These Hash objects are set to last 8 hours (TTL)
 */
public class StreamEventMapProcessorToHash implements StreamEventMapProcessor {

    //make sure to set this value before passing this processor to the Stream Adapter
    private Object callbackTarget = null;
    static AtomicLong counter = new AtomicLong();
    private long sleepTime = 0;

    public StreamEventMapProcessorToHash setSleepTime(long sleepTime)
    {
        this.sleepTime = sleepTime;
        return this;
    }

    void processPoisonPill(Map<String, String> payload){
            System.out.println("processPoisonPill() Received: "+ payload.entrySet().toArray()[0]);
            String id = payload.get("id");
            System.out.println(id);
            writeToRedisHash(":::poisonpill:"+id,
                    "poisonpill","poisonpill");
    }

    @Override
    public void processStreamEventMap(Map<String, StreamEntry> payload) {
        String keyValueString = ""+payload.keySet();
        System.out.println("\nTestEventMapProcessor.processMap()>>\t"+keyValueString);
        doSleep();
        for( String se : payload.keySet()) {
            System.out.println(payload.get(se));
            StreamEntry x = payload.get(se);
            Map<String,String> m = x.getFields();
            String aString = "";
            for( String f : m.keySet()){
                System.out.println("key\t"+f+"\tvalue\t"+m.get(f));
                if(f.equalsIgnoreCase(PAYLOAD_KEYNAME)){
                    String originalString = m.get(f);
                    if(originalString.equalsIgnoreCase("poisonpill")){
                        System.out.println("\n\n\t\tSIMULATING FAILED STATE...");
                        try{
                            Thread.sleep(90000);
                        }catch(InterruptedException is){}
                        throw new NullPointerException("DELIBERATELY KILLING MYSELF ON POISONPILL");
                    }
                    String calcValue = doCalc(originalString);
                    String originalId = se.split(" ")[0];
                    writeToRedisHash(originalId,originalString,calcValue);
                }
            }
        }
    }

    void writeToRedisHash(String originalId,String originalString,String calcString){
        Jedis jedis = null;
        try {
            jedis = JedisConnectionFactory.getInstance().getJedisPool().getResource();
            HashMap<String, String> map = new HashMap<>();
            map.put("arg_provided", originalString);
            map.put("calc_result", calcString);
            map.put(Runtime.getRuntime().toString()+"_Counter",""+counter.incrementAndGet());
            map.put("EntryProvenanceMetaData",originalId);
            map.put("worker_class",this.getClass().getCanonicalName());
            jedis.hset("H:ProcessedEvent:"+originalId,map);
            jedis.pexpire("H:ProcessedEvent:"+originalId,(HISTORY_TIMEOUT_LENGTH_MILLIS));// keep record of processing for 8 hours
        } catch (Throwable t) {
            System.out.println("WARNING:");
            t.printStackTrace();
        } finally {
            jedis.close();
        }
    }

    // write result to a stream in Redis to show progress...
    // goofy example reverses the received string argument client-side and submits it reversed as result
    String doCalc(String aString){
        StringBuffer reversedString = new StringBuffer(aString).reverse();
            return reversedString.toString();
    }

    void doSleep(){
        try{
            Thread.sleep(sleepTime);
        }catch(InterruptedException ie){}
    }

    public void setCallbackTarget(Object callbackTarget){
        this.callbackTarget = callbackTarget;
    }
}