package com.redislabs.sa.ot.streamtest;

import com.redislabs.sa.ot.util.JedisConnectionFactory;
import com.redislabs.sa.ot.util.StreamEventMapProcessor;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.redislabs.sa.ot.streamtest.StreamConstants.OUTPUT_STREAM_NAME;
import static com.redislabs.sa.ot.streamtest.StreamConstants.PAYLOAD_KEYNAME;

/**
 * This class processes Event Entries passed to it by the RedisStreamAdapter
 * It writes Events to a Redis stream that are evidence of the
 * work being completed
 * Some other process needs to clean up the resulting output stream
*/

public class StreamEventMapProcessorToStream implements StreamEventMapProcessor {

    //make sure to set this value before passing this processor to the Stream Adapter
    private Object callbackTarget = null;
    static AtomicLong counter = new AtomicLong();
    private long sleepTime = 0;

    public StreamEventMapProcessorToStream setSleepTime(long sleepTime)
    {
        this.sleepTime = sleepTime;
        return this;
    }

    @Override
    public void processStreamEventMap(Map<String, StreamEntry> payload) {
        printMessageSparingly("\nStreamEventMapProcessorToStream.processStreamEventMap()>>\t" + payload.keySet());
        doSleep(); // the point of this is to simulate slower workers
        for( String se : payload.keySet()) {
            printMessageSparingly(payload.get(se).toString());
            StreamEntry x = payload.get(se);
            Map<String,String> m = x.getFields();
            String aString = "";
            for( String f : m.keySet()){
                printMessageSparingly("key\t"+f+"\tvalue\t"+m.get(f));
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
                    writeToRedisStream(originalId,originalString,calcValue);
                }
            }
        }
    }

    void printMessageSparingly(String message){
        if(counter.get()%10==0) {
            System.out.println(message);
        }
    }

    void writeToRedisStream(String originalId,String originalString,String calcString){
        Jedis jedis = null;
        try {
            jedis = JedisConnectionFactory.getInstance().getJedisPool().getResource();
            HashMap<String, String> map = new HashMap<>();
            map.put("arg_provided", originalString);
            map.put("calc_result", calcString);
            map.put(Runtime.getRuntime().toString()+"_Counter",""+counter.incrementAndGet());
            map.put("EntryProvenanceMetaData",originalId);
            jedis.xadd(OUTPUT_STREAM_NAME, StreamEntryID.NEW_ENTRY,map);
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