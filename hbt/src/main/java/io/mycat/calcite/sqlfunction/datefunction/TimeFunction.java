package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.mycat.MycatBuiltInMethodImpl;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.Duration;
import java.time.LocalDateTime;

public class TimeFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(TimeFunction.class,
            "time");
    public static TimeFunction INSTANCE = new TimeFunction();

    public TimeFunction() {
        super("TIME",
                scalarFunction
        );
    }

    public static Duration time(String time) {
        if (time == null) {
            return null;
        }
       return MycatBuiltInMethodImpl.timeStringToTimeDuration(time);
    }
}
