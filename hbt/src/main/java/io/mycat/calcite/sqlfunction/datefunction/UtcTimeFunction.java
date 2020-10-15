package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;

public class UtcTimeFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(UtcTimeFunction.class,
            "utcTime");
    public static UtcTimeFunction INSTANCE = new UtcTimeFunction();

    public UtcTimeFunction() {
        super("UTC_TIME", scalarFunction);
    }

    public static Duration utcTime(@Parameter(name = "precision", optional = true) Integer precision) {
        if (precision == null) {
            return Duration.ofSeconds(LocalTime.now().toSecondOfDay());
        }
        LocalTime now = LocalTime.now();
        int nano = now.getNano();
        String s = Integer.toString(nano);
        if (s.length() > precision) {
            s = s.substring(0, precision);
            nano = Integer.parseInt(s)*(int)Math.pow(10,9-precision);
        }
        return Duration.ofSeconds(now.toSecondOfDay(), now.getNano());
    }
}

