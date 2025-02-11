package com.wiqer.redis.util;

/**
 * @author lilan
 */
public class TraceIdUtil {

    private static final ThreadLocal<String> TRACE_ID = new ThreadLocal<>();

    private static final Uid uid = WinterId.instance();

    public static String newTraceId() {
        String traceId = uid.generateDigits();
        TRACE_ID.set(traceId);
        return traceId;
    }

    public static String currentTraceId() {
        String result = TRACE_ID.get();
        if (result == null) {
            return newTraceId();
        } else {
            return result;
        }
    }

    public static void bind(String traceId) {
        TRACE_ID.set(traceId);
    }

    public static void clear() {
        TRACE_ID.remove();
    }
}
