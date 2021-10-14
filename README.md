# StreamWorkerGroupProject
An example using Jedis of using Redis Streams - including deliberately failing workers to show Pending Entry Behavior and make us think about what to do...


Some notes at this stage of the project: 2021-10-14

1) Main tells the writer to only write a couple of entries so it is easier to see what is happening. (TODO: this should be a startup argument)

2) I commented out the use of the Reaper - It needs to be reconsidered as pending messages may need special treatment...  
[Also, the command XAUTOCLAIM is not supported by all versions of Redis, (TODO: address this) ]

3) The workers now freeze and eventually throw NullPointer (Die) when a special entry is written like this:
(you can execute this from redis-cli when you are ready)

XADD X:FOR_PROCESSING{xyz} * "stringOffered" "poisonpill"

Once the above entry is added, the worker will die and the associated entry will enter Pending status.  

( you can look for it in the redis-Insights GUI )
 

 
