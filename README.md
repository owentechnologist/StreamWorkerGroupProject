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

#### Non-Default (with 5 args):
* 5th arg Allows you to specify burst sizes (how many of the maximum number to write between brief pauses)



        mvn compile exec:java -Dexec.args="xyz 2 3 1000 10"


#### Non-Default (with any number of args):
* Allows you to specify output=hash as one of the args to the program
* This makes the consumers started for that run use Hashes to record what they process instead of writing the evidence to a stream  
* this sets up the posiibility of later creating an index and using search to look through the processed events

**   (You will have to ensure the search module is installed in your redis instance of course)

Example Usage (works with any of the above argument options):


        mvn compile exec:java -Dexec.args="xyz 2 3 1000 10 output=hash"
        mvn compile exec:java -Dexec.args="output=hash"


### NOTES:

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
that has a keyname beginning with: H:ProcessedEvent::  <-- note the double colon
   

### OTHER INFORMATION ABOUT STREAMS
This link walks the reader through common uses of Streams :

https://stackexchange.github.io/StackExchange.Redis/Streams.html

This link is the formal introduction to the Stream datatype in Redis :

https://redis.io/topics/streams-intro



 

 
