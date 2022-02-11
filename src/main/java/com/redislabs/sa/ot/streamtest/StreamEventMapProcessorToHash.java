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
 *
 * In case you want to try out Redis Search here are some sample commands:
 * FT.CREATE idx:processedEvents PREFIX 1 "H:ProcessedEvent" SCHEMA EntryProvenanceMetaData TEXT arg_provided TEXT calc_result TEXT original_timestamp NUMERIC SORTABLE consumer_process_timestamp NUMERIC SORTABLE source_ip TAG
 * FT.AGGREGATE "idx:processedEvents" "*" GROUPBY 1 @source_ip REDUCE COUNT_DISTINCT 1 arg_provided AS uniqueargs SORTBY 2 @uniqueargs DESC
 * FT.SEARCH "idx:processedEvents" "-@source_ip:{64\\.7\\.94\\.15 | 64\\.7\\.94\\.14}" RETURN 5 source_ip original_timestamp consumer_process_timestamp arg_provided calc_result SORTBY source_ip DESC LIMIT 0 10
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

    void processPoisonPill(Map<String, StreamEntry> payload){
        System.out.println("processPoisonPill() Received: "+ payload.entrySet().toArray()[0]);
        String keyValueString = ""+payload.keySet();
        for( String se : payload.keySet()) {
            System.out.println(payload.get(se));
            StreamEntry x = payload.get(se);
            Map<String, String> m = x.getFields();
            String aString = "";
            for (String f : m.keySet()) {
                System.out.println("key\t" + f + "\tvalue\t" + m.get(f));
                if (f.equalsIgnoreCase(PAYLOAD_KEYNAME)) {
                    String originalString = m.get(f);
                    String calcValue = "UN_PROCESSED";
                    String originalId = se.split(" ")[0];
                    writeToRedisHash(originalId, originalString, calcValue, x.getID().getTime());
                }
            }
        }
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
                    writeToRedisHash(originalId,originalString,calcValue,x.getID().getTime());
                }
            }
        }
    }

    void writeToRedisHash(String originalId,String originalString,String calcString, long orig_timestamp){
        Jedis jedis = null;
        try {
            jedis = JedisConnectionFactory.getInstance().getJedisPool().getResource();
            HashMap<String, String> map = new HashMap<>();
            map.put("arg_provided", originalString);
            map.put("calc_result", calcString);
            map.put(Runtime.getRuntime().toString()+"_Counter",""+counter.incrementAndGet());
            map.put("EntryProvenanceMetaData",originalId);
            map.put("worker_class",this.getClass().getCanonicalName());
            map.put("original_timestamp",""+orig_timestamp);
            map.put("consumer_process_timestamp",System.currentTimeMillis()+"");
            map.put("source_ip",((System.nanoTime()%5)+60)+"."+((System.nanoTime()%5)+3)+"."+((System.nanoTime()%5)+90)+"."+((System.currentTimeMillis()%5)+11));
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