package com.redislabs.sa.ot.streamtest;

public class StreamConstants {

    public static final String DATA_UPDATES_STREAM_BASE = "X:FOR_PROCESSING{";
    public static String DRIVER_STREAM_NAME = "";  // can't be final as it is altered by Main.class based on startup args
    public static final String CONSUMER_GROUP_NAME = "GROUP_ALPHA";
    public static final String PAYLOAD_KEYNAME = "stringOffered";
    public static final long PENDING_MESSAGE_TIMEOUT = 60000; // [milliseconds] used in establishing looping timer for StreamReaper
    public static final long SPEEDY_WORKER_SLEEP_TIME = 10;
    public static final long SLOW_WORKER_SLEEP_TIME = 300;
    public static final String OUTPUT_STREAM_BASE = "X:PROCESSED_EVENTS{";
    public static String OUTPUT_STREAM_NAME = "";  // can't be final as it is altered by Main.class based on startup args
    public static long HISTORY_TIMEOUT_LENGTH_MILLIS = 60*60*1000*8; // 8 hours

}
