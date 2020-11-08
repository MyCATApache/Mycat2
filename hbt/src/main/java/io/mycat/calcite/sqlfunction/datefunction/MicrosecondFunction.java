package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class MicrosecondFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(MicrosecondFunction .class,
            "microsecond");
    public static MicrosecondFunction INSTANCE = new MicrosecondFunction ();

    public MicrosecondFunction() {
        super("MICROSECOND",
                scalarFunction
        );
    }

    public static Long microsecond(Duration duration) {
        boolean negative = duration.isNegative();
        int nano = duration.getNano();
        return TimeUnit.NANOSECONDS.toMicros(nano)* (negative?-1L:1L);
    }
}
