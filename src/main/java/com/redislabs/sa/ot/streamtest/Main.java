package com.redislabs.sa.ot.streamtest;

import com.redislabs.sa.ot.util.JedisConnectionFactory;
import com.redislabs.sa.ot.util.RedisStreamAdapter;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Arrays;

import static com.redislabs.sa.ot.streamtest.StreamConstants.*;

/**
 * Example way to run this Application:
 * Default (no args):
 * # this version uses the default stream key
 * # it also starts up 2 workers  ( id 1 and id 2 )
 * # it also tells the writer to
 * # write 2 entries and then stop
 * # the reaper that trims the output stream and checks for pending work is started as well:
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
 * Non-Default (with any number of args):
 * # You can specify output=hash at any point in the arg list
 * example:
 * mvn compile exec:java -Dexec.args="xyz 2 3 1000 10 output=hash"
 * or:
 * mvn compile exec:java -Dexec.args="output=hash"
 *
 * ^ This means the result of the work performed on the stream events
 * flowing through Redis will be stored for 8 hours in individual Hashes
 * This opens the door for using redis search to search and aggregate the results
 * (You will need to create a search index and be sure the module is installed)
 * *
 * # You can also specify reaper=no at any point in the arg list
 * For example, if you just want to publish events and have no workers or reaper you can use the following:
 * mvn compile exec:java -Dexec.args="xyz 0 0 10000 reaper=no"
 *
 * *
 * To show Pending message behavior use the following command in redis-cli to
 * break one of the workers:
 * XADD X:FOR_PROCESSING{xyz} * "stringOffered" "poisonpill"
 *
 * Once the worker is stuck on the pill, you can check for pending messages like this:
 * XPENDING X:FOR_PROCESSING{xyz} GROUP_ALPHA
 *
 * Note that every 30 seconds the StreamReaper collects pending messages and
 * submits them to a special consumer that can exactly handle our poisonpill
 * PoisonPill process records are stored as Hashes for 8 hours
 * and can be found by scanning for:
 * SCAN H:ProcessedEvent:*
 *
 */

public class Main {

    static final String DEFAULT_UPDATES_STREAM = "X:FOR_PROCESSING{xyz}";
    static final String DEFAULT_OUTPUT_STREAM_NAME = "X:PROCESSED_EVENTS{xyz}";
    static final JedisPool jedisPool = JedisConnectionFactory.getInstance().getJedisPool();
    static final String HASHARG = "output=hash";
    static final String NOREAPER = "reaper=no";
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
        Boolean useHash = false;
        Boolean launchReaper = true; // by default we run the reaper that trims the output stream and checks for poisonpills
        // here is where we apply any passed in args:
        ArrayList<String> tempArgs = new ArrayList<String>();
        if(Arrays.asList(args).contains(HASHARG)||Arrays.asList(args).contains(NOREAPER)){
            for(String e:args) {
                if(e.equalsIgnoreCase(HASHARG)){
                    //do not add to the new array
                    useHash=true;
                }else if(e.equalsIgnoreCase(NOREAPER)) {
                    launchReaper = false;
                }else{
                    tempArgs.add(e);
                }
            }
            args = (String[]) tempArgs.toArray(new String[0]);
            System.out.println(args.length);
        }
        if(args.length>0){
            dataUpdatesStreamTarget  = StreamConstants.DATA_UPDATES_STREAM_BASE+args[0]+"}";
            outputStreamName = StreamConstants.OUTPUT_STREAM_BASE+args[0]+"}";
            if(args.length>2){
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
        StreamConstants.OUTPUT_STREAM_NAME = outputStreamName;
        StreamConstants.DRIVER_STREAM_NAME = dataUpdatesStreamTarget;

        try{
            Thread.sleep(500); // give the writer time to establish some messages
        }catch(Throwable t){}
        if(useHash){
            kickOffStreamAdapterWithHashResponse();
        }else {
            kickOffStreamAdapterWithStreamResponse();
        }
        if(launchReaper) {
            StreamReaper reaper = new StreamReaper(dataUpdatesStreamTarget, jedisPool);
            reaper.kickOffStreamReaping(StreamConstants.PENDING_MESSAGE_TIMEOUT, StreamConstants.CONSUMER_GROUP_NAME);
        }
        //NB: when using consumer groups anything written to stream before the worker starts listening
        // will not be detected if using StreamEntryID.UNRECEIVED_ENTRY.
        // so we save starting up the writer to be the last task:
        StreamWriter writer = new StreamWriter(StreamConstants.DRIVER_STREAM_NAME,jedisPool);
        writer.kickOffStreamEvents(maxEntriesForWriter,batchSizeBetweenWriterPauses);
    }

    private static void kickOffStreamAdapterWithStreamResponse() {
        RedisStreamAdapter streamAdapter = new RedisStreamAdapter(StreamConstants.DRIVER_STREAM_NAME,jedisPool);
        streamAdapter.createConsumerGroup(StreamConstants.CONSUMER_GROUP_NAME);
        for(int x=0;x<numWorkers;x++){
            int workerID = workerStartId+x;
            if(x%2==0) {
                // pass the StreamAdapter a MapProcessor to do some work
                streamAdapter.namedGroupConsumerStartListening("" +workerID,
                        new StreamEventMapProcessorToStream().setSleepTime(StreamConstants.SLOW_WORKER_SLEEP_TIME));
            }else{
                // pass the StreamAdapter a MapProcessor to do some work
                streamAdapter.namedGroupConsumerStartListening("" + workerID,
                        new StreamEventMapProcessorToStream().setSleepTime(StreamConstants.SPEEDY_WORKER_SLEEP_TIME));
            }
        }
    }
    private static void kickOffStreamAdapterWithHashResponse() {
        RedisStreamAdapter streamAdapter = new RedisStreamAdapter(StreamConstants.DRIVER_STREAM_NAME,jedisPool);
        streamAdapter.createConsumerGroup(StreamConstants.CONSUMER_GROUP_NAME);
        for(int x=0;x<numWorkers;x++){
            int workerID = workerStartId+x;
            if(x%2==0) {
                // pass the StreamAdapter a MapProcessor to do some work
                streamAdapter.namedGroupConsumerStartListening("" +workerID,
                        new StreamEventMapProcessorToHash().setSleepTime(StreamConstants.SLOW_WORKER_SLEEP_TIME));
            }else{
                // pass the StreamAdapter a MapProcessor to do some work
                streamAdapter.namedGroupConsumerStartListening("" + workerID,
                        new StreamEventMapProcessorToHash().setSleepTime(StreamConstants.SPEEDY_WORKER_SLEEP_TIME));
            }
        }
    }

}
