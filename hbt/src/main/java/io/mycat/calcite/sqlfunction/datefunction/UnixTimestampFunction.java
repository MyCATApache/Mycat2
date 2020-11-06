package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Locale;

public class UnixTimestampFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(UnixTimestampFunction.class,
            "unixTimestamp");
    public static UnixTimestampFunction INSTANCE = new UnixTimestampFunction();

    public UnixTimestampFunction() {
        super("UNIX_TIMESTAMP", scalarFunction);
    }

    public static long unixTimestamp(@Parameter(name = "date", optional = true) LocalDateTime date) {
        if (date == null) {
            return System.currentTimeMillis() / 1000;
        } else {
            return Timestamp.valueOf(date).getTime() / 1000;
        }
    }
}

