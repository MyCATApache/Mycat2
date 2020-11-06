package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.Duration;
import java.time.LocalTime;

public class SecToTimeFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(SecToTimeFunction.class,
            "secToTime");
    public static SecToTimeFunction INSTANCE = new SecToTimeFunction();

    public SecToTimeFunction() {
        super("SEC_TO_TIME",
                scalarFunction
        );
    }

    public static Duration secToTime(Long second) {
        if (second == null) {
            return null;
        }
        return Duration.ofSeconds(second);
    }
}
