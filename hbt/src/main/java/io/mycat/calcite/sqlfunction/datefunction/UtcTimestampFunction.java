package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class UtcTimestampFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(UtcTimestampFunction.class,
            "utcTimestamp");
    public static UtcTimestampFunction INSTANCE = new UtcTimestampFunction();

    public UtcTimestampFunction() {
        super("UTC_TIMESTAMP", scalarFunction);
    }

    public static LocalDateTime utcTimestamp(@Parameter(name = "precision", optional = true) Integer precision) {
        if (precision == null) {
            return LocalDateTime.now();
        }
        LocalDateTime now = LocalDateTime.now();
        int nano = now.getNano();
        String s = Integer.toString(nano);
        if (s.length() > precision) {
            s = s.substring(0, precision);
            nano = Integer.parseInt(s)*(int)Math.pow(10,9-precision);
        }
        return now.withNano(nano);
    }
}

