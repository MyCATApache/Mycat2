package io.vertx;

import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.StringJoiner;
import java.util.function.Consumer;

@Getter
@Setter
public class TraceTimeoutSpan extends TraceSpan {
    protected final Object traceObject;
    protected long createTimestamp;
    protected long expiryTimestamp;
    protected Consumer<TraceTimeoutSpan> onClose;

    public TraceTimeoutSpan(Thread createBy, long traceId, StackTraceElement[] stackTrace, Object traceObject, Consumer<TraceTimeoutSpan> onClose) {
        super(createBy, traceId, stackTrace);
        this.traceObject = traceObject;
        this.onClose = onClose;
    }

    public void close() {
        onClose.accept(this);
    }

    @Override
    public String toLogString() {
        StackTraceElement[] shortStack = Arrays.copyOfRange(stackTrace, 1, Math.min(8, stackTrace.length));
        StringJoiner stackTraceJoiner = new StringJoiner("\t");
        for (StackTraceElement element : shortStack) {
            stackTraceJoiner.add(element.toString());
        }
        return traceId +
                "\t" + fixLength(createBy.getName(), 12) +
                "\t" + fixLength(stackTrace[2].toString(), 100) +
                "\t" + fixLength(String.valueOf(traceObject), 100) +
                "\t" + stackTraceJoiner;
    }
}
