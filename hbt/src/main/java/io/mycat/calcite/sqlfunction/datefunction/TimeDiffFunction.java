package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class TimeDiffFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(TimeDiffFunction.class,
            "timdDiff");
    public static TimeDiffFunction INSTANCE = new TimeDiffFunction();

    public TimeDiffFunction() {
        super("TIMEDIFF", scalarFunction);
    }

    public static Duration timdDiff(LocalDateTime date1, LocalDateTime date2) {
        if (date1 == null||date2 == null){
            return null;
        }
        return Duration.between(date2,date1);
    }
}
