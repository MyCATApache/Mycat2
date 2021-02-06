package io.vertx;

import io.netty.util.concurrent.FastThreadLocal;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public enum Trace {
    /**/
    GLOBAL;
    private final AtomicLong traceIdIncr = new AtomicLong();
    @Getter
    private final Queue<TraceSpan> queue = new ConcurrentLinkedQueue<>();

    private static final boolean ENABLE = Boolean.getBoolean("trace.enable");
    private static final FastThreadLocal<TraceContext> CONTEXT_THREAD_LOCAL = new FastThreadLocal<TraceContext>() {
        @Override
        protected TraceContext initialValue() throws Exception {
            return new TraceContext();
        }
    };

    public static TraceContext getContext() {
        return CONTEXT_THREAD_LOCAL.get();
    }

    public static void removetContext() {
        CONTEXT_THREAD_LOCAL.remove();
    }

    public static boolean isEnable() {
        return true;
//        return ENABLE;
    }

    static {
        if (isEnable()) {
            TraceLogFileThread thread = new TraceLogFileThread("trace-dump");
            thread.setPriority(Thread.MIN_PRIORITY);
            thread.start();
        }
    }

    public TraceSpan span() {
        if (isEnable()) {
            StackTraceElement[] stackTrace = stackTrace();
            TraceSpan span = new TraceSpan(currentThread(), traceId(), Arrays.copyOfRange(stackTrace,2,stackTrace.length));
            queue.offer(span);
            return span;
        } else {
            return TraceSpan.EMPTY;
        }
    }

    private Thread currentThread() {
        if (isEnable()) {
            return Thread.currentThread();
        } else {
            return TraceSpan.EMPTY.getCreateBy();
        }
    }

    private long traceId() {
        if (isEnable()) {
            return traceIdIncr.getAndIncrement();
        } else {
            return TraceSpan.EMPTY.getTraceId();
        }
    }

    private StackTraceElement[] stackTrace() {
        if (isEnable()) {
            return new Throwable().getStackTrace();
        } else {
            return TraceSpan.EMPTY.getStackTrace();
        }
    }


}
