package io.vertx;

import io.mycat.util.ExpiryLRUMap;
import io.netty.util.concurrent.FastThreadLocal;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Slf4j
public enum Trace {
    /*追踪分类*/

    /**
     * 全量日志记录
     */
    GLOBAL,
    /**
     * 仅记录超时
     */
    TIMEOUT;

    private final AtomicLong traceIdIncr = new AtomicLong();
    @Getter
    private final Queue<TraceSpan> queue = new ConcurrentLinkedQueue<>();

    private final ExpiryLRUMap<Long, TraceTimeoutSpan> expiryLRUMap = new ExpiryLRUMap<>();
    private final Consumer<TraceTimeoutSpan> onCloseTimeout = span -> {
        expiryLRUMap.remove(span.getTraceId());
    };

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

    Trace() {
        expiryLRUMap.setReplaceNullValueFlag(true);
        expiryLRUMap.setOnExpiryConsumer(node -> {
            TraceTimeoutSpan span = node.getData();
            span.setCreateTimestamp(node.getCreateTimestamp());
            span.setExpiryTimestamp(node.getExpiryTimestamp());
            queue.offer(span);
        });
    }

    static {
        if (isEnable()) {
            TraceLogFileThread thread = new TraceLogFileThread("trace-dump");
            thread.setPriority(Thread.MIN_PRIORITY);
            thread.start();
        }
    }

    /**
     * 超时追踪(仅超时5秒. 则会计入日志)
     *
     * @param traceObject 追踪对象
     * @return 在规定时间内, 没有调用close方法, 将会被记录超时日志. {@link TraceTimeoutSpan#close()}
     */
    public TraceTimeoutSpan spanTimeoutPromise(Object traceObject) {
        StackTraceElement[] stackTrace = stackTrace();
        TraceTimeoutSpan span = new TraceTimeoutSpan(currentThread(), traceId(), Arrays.copyOfRange(stackTrace, 2, stackTrace.length),
                traceObject, onCloseTimeout);
        expiryLRUMap.put(span.getTraceId(), span, 5000);
        return span;
    }

    /**
     * 超时追踪(仅超时)
     *
     * @param traceObject 追踪对象
     * @param timeout     毫秒
     * @return 在规定时间内, 没有调用close方法, 将会被记录超时日志. {@link TraceTimeoutSpan#close()}
     */
    public TraceTimeoutSpan spanTimeout(Object traceObject, int timeout) {
        StackTraceElement[] stackTrace = stackTrace();
        TraceTimeoutSpan span = new TraceTimeoutSpan(currentThread(), traceId(), Arrays.copyOfRange(stackTrace, 2, stackTrace.length),
                traceObject, onCloseTimeout);
        expiryLRUMap.put(span.getTraceId(), span, timeout);
        return span;
    }

    /**
     * 调用链追踪 (全量)
     *
     * @return
     */
    public TraceSpan span() {
        if (isEnable()) {
            StackTraceElement[] stackTrace = stackTrace();
            TraceSpan span = new TraceSpan(currentThread(), traceId(), Arrays.copyOfRange(stackTrace, 2, stackTrace.length));
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
