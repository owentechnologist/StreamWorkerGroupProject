package com.redislabs.sa.ot.streamtest;

import com.redislabs.sa.ot.util.JedisConnectionFactory;
import com.redislabs.sa.ot.util.RedisStreamAdapter;
import redis.clients.jedis.JedisPool;

import static com.redislabs.sa.ot.streamtest.StreamConstants.*;

/**
 * Example way to run this Application:
 * Default (no args):
 * # this version uses the default stream key
 * # it also starts up 2 workers  ( id 1 and id 2 )
 * # it also tells the writer to
 * # write 2 entries and then stop:
 * mvn compile exec:java
 *
 * Non-Default (with 3 args):
 * # Allows you to specify an alternate StreamID modifier as the first arg (for scaling)
 * # Use 'xyz' to keep the same stream as the default
 * # The second arg is the number of workers to start during this run
 * # The third arg is am integer used as the lowest id to be used for worker ids in this run
 * #
 * mvn compile exec:java -Dexec.args="xyz 2 3"
 * mvn compile exec:java -Dexec.args="xyz 4 5"
 *
 * Non-Default (with 4 args):
 * # 4th arg Allows you to specify the maximum number of entries for the writer to produce
 * mvn compile exec:java -Dexec.args="xyz 2 3 1000"
 *
 * Non-Default (with 5 args):
 * # 5th arg Allows you to specify the number of entries to write as fast as possible before taking a brief pause
 * mvn compile exec:java -Dexec.args="xyz 2 3 1000 10"
 *
 * To show Pending message behavior use the following command in redis-cli to break one of the workers:
 * XADD X:FOR_PROCESSING{xyz} * "stringOffered" "poisonpill"
 *
 * Once the worker is stuck on the pill, you can check for pending messages like this:
 * XPENDING
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
    static int numWorkers = 2;
    static int workerStartId = 1;
    static long maxEntriesForWriter = 2;
    static long batchSizeBetweenWriterPauses = 2;
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
                numWorkers = Integer.parseInt(args[1]);
                workerStartId = Integer.parseInt(args[2]);
            }
            if(args.length>3){
                maxEntriesForWriter = Integer.parseInt(args[3]);
            }
            if(args.length>4){
                batchSizeBetweenWriterPauses = Integer.parseInt(args[4]);
            }
        }
        OUTPUT_STREAM_NAME = outputStreamName;
        DRIVER_STREAM_NAME = dataUpdatesStreamTarget;

        try{
            Thread.sleep(500); // give the writer time to establish some messages
        }catch(Throwable t){}

        RedisStreamAdapter streamAdapter = new RedisStreamAdapter(DRIVER_STREAM_NAME,jedisPool);
        streamAdapter.createConsumerGroup(CONSUMER_GROUP_NAME);
        for(int x=0;x<numWorkers;x++){
            int workerID = workerStartId+x;
            if(x%2==0) {
                streamAdapter.namedGroupConsumerStartListening("" +workerID,
                        new TestStreamEventMapProcessor().setSleepTime(SLOW_WORKER_SLEEP_TIME));
            }else{
                streamAdapter.namedGroupConsumerStartListening("" + workerID,
                        new TestStreamEventMapProcessor().setSleepTime(SPEEDY_WORKER_SLEEP_TIME));
            }
        }
        //StreamReaper reaper = new StreamReaper(dataUpdatesStreamTarget,jedisPool);
        //reaper.kickOffStreamReaping(PENDING_MESSAGE_TIMEOUT,CONSUMER_GROUP_NAME);
        //NB: when using consumer groups anything written to stream before the worker starts listening
        // will not be detected if using StreamEntryID.UNRECEIVED_ENTRY.
        // so we save starting up the writer to be the last task:
        StreamWriter writer = new StreamWriter(DRIVER_STREAM_NAME,jedisPool);
        writer.kickOffStreamEvents(maxEntriesForWriter,batchSizeBetweenWriterPauses);
    }

}
