package io.vertx;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class TraceSpan {
    private final Thread createBy;
    private final long traceId;
    private final StackTraceElement[] stackTrace;

    @Override
    public String toString() {
        return "traceId=" + traceId + ",createBy=" + createBy + "," + stackTrace[0];
    }

    public static final TraceSpan EMPTY = new TraceSpan(Thread.currentThread(), 0, new StackTraceElement[0]) {
        @Override
        public String toString() {
            return "EMPTY";
        }
    };

}