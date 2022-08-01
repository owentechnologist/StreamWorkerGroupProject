# StreamWorkerGroupProject
An example using Jedis of using Redis Streams - 
including deliberately failing workers to show Pending Entry Behavior 
and make us think about what to do...

This application starts a writer thread and two worker threads.

* The payload written in the entry is a simple string. 
  * It has the keyname: 'stringOffered'

* The workers 
    * reverse the string
    * write the original version and the reversed version to an output Stream X:streamActivity{xyz}
    * they also include the runtime ID, so you can distinguish between multiple processes (to see differing Runtime IDs, you would have to execute this program more than one time) 
    * They also write the 'EntryProvenanceMetaData' which is a composite value separated by : composed of
        * The Stream Name from which the original entry came
        * The ID for the Entry when it was written to that first stream (the timestamp)
        * The ConsumerName (workerID) of the worker consuming that entry 
          
This EntryProvenanceMetaData looks like this: (here we see worker '2' processed this entry)  
          
    X:FOR_PROCESSING{xyz}:1634234454312-0:2


## How to start the Application:

#### Default (no args):
* this version uses the default stream key
* it also starts up 2 workers  ( id 1 and id 2 )
* it also tells the writer to write 2 entries and then stop:
  

    mvn compile exec:java


#### Non-Default (with 3 args):
* Allows you to specify an alternate StreamID modifier as the first arg (for scaling)
* Use 'xyz' to keep the same stream as the default
* The second arg is the number of workers to start during this run
* The third arg is an integer used as the lowest id to be used for worker ids in this run

        mvn compile exec:java -Dexec.args="xyz 2 3"

        mvn compile exec:java -Dexec.args="xyz 4 5"


#### Non-Default (with 4 args):
* 4th arg Allows you to specify the maximum number of entries for the writer to produce


        mvn compile exec:java -Dexec.args="xyz 2 3 1000"

NB: If you provide the value 0 for number of workers AND 0 for the start id
- the program will only write messages (act as a publisher) and no new workers will be added to the worker group


        mvn compile exec:java -Dexec.args="xyz 0 0 10000"

#### Non-Default (with 5 args):
* 5th arg Allows you to specify burst sizes (how many of the maximum number to write between brief pauses)

        mvn compile exec:java -Dexec.args="xyz 2 3 1000 10"


#### Non-Default (with any number of args):
* Another option is to NOT launch the Reaper thread that trims the output stream and checks for pending messages
  (these will usually be poisonpill messages)
```
        mvn compile exec:java -Dexec.args="xyz 0 0 10000 reaper=no"
        mvn compile exec:java -Dexec.args="reaper=no"
```

* Allows you to specify output=hash as one of the args to the program
* This makes the consumers started for that run use Hashes to record what they process instead of writing the evidence to a stream  
* this sets up the posiibility of later creating an index and using search to look through the processed events

**   (You will have to ensure the search module is installed in your redis instance of course)

Example Usage (works with any of the above argument options):


        mvn compile exec:java -Dexec.args="xyz 2 3 1000 10 output=hash"
        mvn compile exec:java -Dexec.args="output=hash"
        mvn compile exec:java -Dexec.args="xyz 1 3 10000 reaper=no output=hash"

If you do decide to use RediSearch here are some sample commands to create an index and query it: 

    FT.CREATE idx_processedEvents PREFIX 1 "H:ProcessedEvent" SCHEMA EntryProvenanceMetaData TEXT arg_provided TEXT calc_result TEXT original_timestamp NUMERIC SORTABLE consumer_process_timestamp NUMERIC SORTABLE source_ip TAG

    FT.AGGREGATE "idx_processedEvents" "*" GROUPBY 1 @source_ip REDUCE COUNT_DISTINCT 1 arg_provided AS uniqueargs SORTBY 2 @uniqueargs DESC

    FT.SEARCH "idx_processedEvents" "-@source_ip:{64\\.7\\.94\\.15 | 64\\.7\\.94\\.14}" RETURN 5 source_ip original_timestamp consumer_process_timestamp arg_provided calc_result SORTBY source_ip DESC LIMIT 0 10
    
A search query that returns the number of events processed by each worker id would be this:

``` 
FT.AGGREGATE "idx_processedEvents" "*" LOAD 1 @EntryProvenanceMetaData APPLY "substr(@EntryProvenanceMetaData, -1, 1)" AS WORKER_ID GROUPBY 1 @WORKER_ID REDUCE COUNT 0 AS WORK_By_WorkerID
```

The above query results in output like this (when there are 3 workers):

``` 
1) "3"
2) 1) "WORKER_ID"
   2) "3"
   3) "WORK_By_WorkerID"
   4) "5936"
3) 1) "WORKER_ID"
   2) "2"
   3) "WORK_By_WorkerID"
   4) "3678"
4) 1) "WORKER_ID"
   2) "1"
   3) "WORK_By_WorkerID"
   4) "386"
```

### POISON PILL NOTES:

* A consumer will freeze and eventually throw NullPointer (Die) when a special entry is written like this:
  *  (you can execute this from redis-cli once for each consumer you want to poison)


        XADD X:FOR_PROCESSING{xyz} * "stringOffered" "poisonpill"

Once the above entry is added, the worker that consumes it will die and the associated entry will enter Pending status.

You can look for pending entries in the redis-Insights GUI or by issuing this command from redis-cli :

    XPENDING X:FOR_PROCESSING{xyz} GROUP_ALPHA
1) (integer) 1
2) "1634197477177-0"
3) "1634197477177-0"
4) 1) 1) "6"
2) "1"


#### Beware the StreamReaper . . .  (joke)
The StreamReaper class does 2 things every 30 seconds:
1) executes XTRIM on the outbound work stream leaving only the last 100 entries 
2) Scans for Pending Stream Entries and cleans up any that contain a poisonpill

**  #2 above  means you only have 30 seconds to see any poison pills in the stream 
-- note that evidence of processing the poisonpill will exist as a Hash written to Redis
that has a keyname beginning with: H:ProcessedEvent::  <-- note the double colon which differentiates the poison from regular processed events

To avoid feeling rushed, you can (as mentioned above) provide the startup argument reaper=no to launch the program 
without a reaper
   
### To clean up the hashes from a run where many have been written, you can execute the following lua script from redis-cli:

```

EVAL "local cursor = 0 local keyNum = 0 repeat local res = redis.call('scan',cursor,'MATCH',KEYS[1]..'*') if(res ~= nil and #res>=0) then cursor = tonumber(res[1]) local ks = res[2] if(ks ~= nil and #ks>0) then for i=1,#ks,1 do local key = tostring(ks[i]) redis.call('UNLINK',key) end keyNum = keyNum + #ks end end until( cursor <= 0 ) return keyNum" 1 H:Pr

```

### OTHER INFORMATION ABOUT STREAMS
This link walks the reader through common uses of Streams :

https://stackexchange.github.io/StackExchange.Redis/Streams.html

This link is the formal introduction to the Stream datatype in Redis :

https://redis.io/topics/streams-intro



 

 
