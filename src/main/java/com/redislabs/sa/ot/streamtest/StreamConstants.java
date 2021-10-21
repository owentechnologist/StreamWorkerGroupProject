package com.redislabs.sa.ot.streamtest;

public class StreamConstants {

    public static final String DATA_UPDATES_STREAM_BASE = "X:FOR_PROCESSING{";
    public static final String CONSUMER_GROUP_NAME = "GROUP_ALPHA";
    public static final String PAYLOAD_KEYNAME = "stringOffered";
    public static final long PENDING_MESSAGE_TIMEOUT = 100;
    public static final long SPEEDY_WORKER_SLEEP_TIME = 10;
    public static final long SLOW_WORKER_SLEEP_TIME = 300;
    public static String DRIVER_STREAM_NAME = "";  // can't be final as it may be altered by Main.class
    public static final String OUTPUT_STREAM_BASE = "X:streamActivity{";  // can't be final as it may be altered by Main.class
    public static String OUTPUT_STREAM_NAME = "X:streamActivity{";  // can't be final as it may be altered by Main.class
    public static long HISTORY_TIMEOUT_LENGTH_MILLIS = 60*60*1000*8; // 8 hours

}
