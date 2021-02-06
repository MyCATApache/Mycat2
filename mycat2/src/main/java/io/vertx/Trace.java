package io.vertx;

import io.netty.util.concurrent.FastThreadLocal;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

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
        return ENABLE;
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
            TraceSpan span = new TraceSpan(currentThread(), traceId(), stackTrace());
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
            return null;
        }
    }

    private long traceId() {
        if (isEnable()) {
            return traceIdIncr.getAndIncrement();
        } else {
            return 0;
        }
    }

    private StackTraceElement[] stackTrace() {
        if (isEnable()) {
            return new Throwable().getStackTrace();
        } else {
            return null;
        }
    }


}
