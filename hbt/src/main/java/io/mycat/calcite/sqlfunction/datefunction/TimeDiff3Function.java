package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.mycat.MycatBuiltInMethodImpl;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.Duration;
import java.time.temporal.Temporal;

public class TimeDiff3Function extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(TimeDiff3Function.class,
            "timdDiff");
    public static TimeDiff3Function INSTANCE = new TimeDiff3Function();

    public TimeDiff3Function() {
        super("TIMEDIFF", scalarFunction);
    }

    public static Duration timdDiff(Duration date1, Duration date2) {
        if (date1 == null||date2 == null){
            return null;
        }
        return date1.minus(date2);
    }
}
