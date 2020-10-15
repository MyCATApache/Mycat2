package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.Duration;
import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

public class MinuteFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(MinuteFunction.class,
            "minute");
    public static MinuteFunction INSTANCE = new MinuteFunction();

    public MinuteFunction() {
        super("MINUTE",
                scalarFunction
        );
    }

    public static Long minute(Duration duration) {
        boolean negative = duration.isNegative();
        return LocalTime.ofSecondOfDay(duration.getSeconds()).getMinute()
                * (negative?-1L:1L);
    }
}
