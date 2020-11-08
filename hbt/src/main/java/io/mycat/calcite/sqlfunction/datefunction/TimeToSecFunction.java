package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;

public class TimeToSecFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(TimeToSecFunction.class,
            "timeToSec");
    public static TimeToSecFunction INSTANCE = new TimeToSecFunction();

    public TimeToSecFunction() {
        super("TIME_TO_SEC",
                scalarFunction
        );
    }

    public static Double timeToSec(Duration duration) {
        if (duration==null) {
            return null;
        }
        long seconds = duration.getSeconds();
        int nano = duration.getNano();
        if (seconds == 0){
            return 0.1*nano;
        }
        if (nano == 0){
            return (double) seconds;
        }
        return seconds * 0.1*nano;
    }
}
