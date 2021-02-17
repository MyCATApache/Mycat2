package io.vertx;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.StringJoiner;

@Getter
@AllArgsConstructor
public class TraceSpan {
    protected final Thread createBy;
    protected final long traceId;
    protected final StackTraceElement[] stackTrace;

    @Override
    public String toString() {
        return "traceId=" + traceId + ",createBy=" + createBy.getName() + "," + stackTrace[2];
    }

    public String toLogString() {
        StackTraceElement[] shortStack = Arrays.copyOfRange(stackTrace, 1, Math.min(8, stackTrace.length));
        StringJoiner stackTraceJoiner = new StringJoiner("\t");
        for (StackTraceElement element : shortStack) {
            stackTraceJoiner.add(element.toString());
        }
        return traceId +
                "\t" + fixLength(createBy.getName(), 12) +
                "\t" + fixLength(stackTrace[2].toString(), 100) +
                "\t" + stackTraceJoiner;
    }

    protected String fixLength(String str, int length) {
        StringBuilder createByName = new StringBuilder(str);
        while (createByName.length() < length) {
            createByName.append(" ");
        }
        return createByName.substring(0, Math.min(length, createByName.length()));
    }

    public static final TraceSpan EMPTY = new TraceSpan(Thread.currentThread(), 0, new StackTraceElement[0]) {
        @Override
        public String toString() {
            return "EMPTY";
        }
    };

}