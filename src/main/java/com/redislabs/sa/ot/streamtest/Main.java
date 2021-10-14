package com.redislabs.sa.ot.streamtest;

import com.redislabs.sa.ot.util.JedisConnectionFactory;
import com.redislabs.sa.ot.util.RedisStreamAdapter;
import redis.clients.jedis.JedisPool;

import static com.redislabs.sa.ot.streamtest.StreamConstants.*;

/**
 * Example way to run this Application:
 * mvn compile exec:java -Dexec.args="xyz 3"
 * mvn compile exec:java -Dexec.args="xyz 5"
 *
 * Where "ddk" is the stream routing value and "3" is the starting value for the two
 * worker IDs to be used
 *
 * To show Pending message use the following command in redis-cli to break one of the workers:
 * xadd X:FOR_PROCESSING{xyz} * "stringOffered" "poisonpill"
 *
 * With this in mind - it may be best to make the reaper behave differently
 * - maybe it collects pending messages and submits them to a special consumer that can handle poison
 * (Currently the reaper just passes any pending messages found to worker #1)
 * Reaper is commented out - now that the poison pill test exists
 *
 */

public class Main {

    static final String DEFAULT_UPDATES_STREAM = "X:FOR_PROCESSING{xyz}";
    static final String DEFAULT_OUTPUT_STREAM_NAME = "X:streamActivity{xyz}";
    static final JedisPool jedisPool = JedisConnectionFactory.getInstance().getJedisPool();
    static Main main = null;
    static int workerStartId = 1;

    //Pass in the preferred routing value to use when listening for stream events
    //examples: ddk  okg   pkg
    //Hint: you can browse for keys starting with X:FOR_PROCESSING and use one of their routing values
    // Second argument can also be passed in - which is the starting number for the two workers that will be started
    public static void main(String[] args){
        String dataUpdatesStreamTarget = DEFAULT_UPDATES_STREAM;
        String outputStreamName = DEFAULT_OUTPUT_STREAM_NAME;
        // here is where we apply any passed in args:
        if(args.length>0){
            dataUpdatesStreamTarget  = DATA_UPDATES_STREAM_BASE+args[0]+"}";
            outputStreamName = OUTPUT_STREAM_BASE+args[0]+"}";
            if(args.length>1){
                workerStartId = Integer.parseInt(args[1]);
            }
        }
        OUTPUT_STREAM_NAME = outputStreamName;
        DRIVER_STREAM_NAME = dataUpdatesStreamTarget;


        try{
            Thread.sleep(500); // give the writer time to establish some messages
        }catch(Throwable t){}

        RedisStreamAdapter streamAdapter = new RedisStreamAdapter(DRIVER_STREAM_NAME,jedisPool);
        streamAdapter.createConsumerGroup(CONSUMER_GROUP_NAME);
        TestStreamEventMapProcessor speedy = new TestStreamEventMapProcessor();
        streamAdapter.namedGroupConsumerStartListening(""+workerStartId,speedy);
        TestStreamEventMapProcessor slowPoke = new TestStreamEventMapProcessor();
        slowPoke.setSleepTime(PENDING_MESSAGE_TIMEOUT);
        streamAdapter.namedGroupConsumerStartListening(""+(workerStartId+1),slowPoke);
        //StreamReaper reaper = new StreamReaper(dataUpdatesStreamTarget,jedisPool);
        //reaper.kickOffStreamReaping(PENDING_MESSAGE_TIMEOUT,CONSUMER_GROUP_NAME);
        //when using consumer groups anything written to stream will not be detected if using StreamEntryID.UNRECEIVED_ENTRY.
        StreamWriter writer = new StreamWriter(DRIVER_STREAM_NAME,jedisPool);
        writer.kickOffStreamEvents(2,2);
    }

}
