import datetime
import json

## This file should be registered as a gear with the redis instance/cluster involved
## Make sure this gear is registered if you want to see it in action
## use the RedisInsight tool for simple upload, activation, and monitoring
## https://redislabs.com/redis-enterprise/redis-insight/

# This gear is registered to listen for events on the stream: X:STREAM:SUBMISSIONS{groupA}
# these stream events are expected to contain strings that can be 'processed'
# 1) 1621846248588-0
#     2) 1) spellCheckMe
#        2) burnabee
#        3) requestID
#        4) PM_UID75439582505275
# this gear
# 1. checks to see if the provided city-word has been checked before using the Redis Set data type (Set name is determined by first 3 letters in argument)
# 2. writes unique and unchecked city names to a targeted Stream for processing X:FOR_PROCESSING{routeValue}
# stream data looks like this:
# {\"key\": \"X:STREAM:SUBMISSIONS{groupA}\", \"id\": \"1621833630358-0\", \"value\": {\"requestID\": \"PM_UID4\", \"spellCheckMe\": \"tauranto\"}}
# Below are a couple of lua scripts to generate bogus city names (the first LUA script does this)
# and trigger this gear (the second LUA script does this)

# EVAL "for index = 1,10000,1 do redis.call('SADD',KEYS[1],(string.char((index%20)+97)) .. string.char(((index+7)%11)+97) .. string.char(((index+3)%11)+97) .. 'SomeCity' .. index ) end" 1 s:cityList{groupA}
# EVAL "for index = 1,10000,1 do redis.call('XADD',KEYS[2],'*','requestID',index,'spellCheckMe',redis.call('SRANDMEMBER',KEYS[1])) end" 2 s:cityList{groupA} X:STREAM:SUBMISSIONS{groupA}


# to repeatedly test with the streamtest.Main Java Program:
##  delete one of the Set data structures indicating previously seen city name requests: s:addr:hits:{pkg}
## and delete one of the Streams indicating work to be processed X:FOR_PROCESSING{pkg}

## fire off the Main Java program to start the Stream processor
## run the following LUA script:  (this assumes you have run the first script above to create a bunch of cityNames already)
# EVAL "for index = 1,30000,1 do redis.call('XADD',KEYS[2],'*','requestID',index,'spellCheckMe',redis.call('SRANDMEMBER',KEYS[1])) end" 2 s:cityList{groupA} X:GBG:CITY{groupA}
# to submit a single request you can issue this sort of thing:
#  XADD X:GBG:CITY{groupA} * spellCheckMe pkgSomeCity6478 requestID 1

def dedupAndAssign(starttime,s1):
  jsonVersion =json.loads(s1)
  cityName = jsonVersion['value']['spellCheckMe']
  requestID = jsonVersion['value']['requestID']
  routingPrefix = cityName[0]+cityName[1]+cityName[2]
  #check for duplicate (if it exists we will see a value > 0)
  if execute('SISMEMBER','s:addr:hits:{'+routingPrefix+'}',cityName) == 0:
    execute('SADD','s:addr:hits:{'+routingPrefix+'}',cityName)
    duration = datetime.datetime.now()-starttime
    execute('XADD','X:FOR_PROCESSING{'+routingPrefix+'}','*','stringDelivered',cityName,'processTimeDuration: ',duration)
  else:
    a = 1
    #do nothing

# GB is the GearBuilder (a factory for gears)
s_gear = GB('StreamReader',desc = "When a String is submitted - dedup and partition the requests for downstream processing" )
#class GearsBuilder('StreamReader').run(prefix='*', batch=1, duration=0, onFailedPolicy='continue', onFailedRetryInterval=1, trimStream=True)
s_gear.foreach(
  lambda x: dedupAndAssign(datetime.datetime.now(),json.dumps(x))
)

s_gear.register(
    'X:STREAM:SUBMISSIONS{groupA}',
    trimStream=False
    # setting trimStream=True  causes this gear to remove events from the stream it listens to as it processes them
)
