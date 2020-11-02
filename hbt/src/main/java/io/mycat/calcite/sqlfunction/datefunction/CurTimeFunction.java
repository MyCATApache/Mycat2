package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.Duration;
import java.time.LocalTime;

public class CurTimeFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(CurTimeFunction.class,
            "curTime");
    public static CurTimeFunction INSTANCE = new CurTimeFunction();

    public CurTimeFunction() {
        super("CURTIME",
                scalarFunction
        );
    }

    public static Duration curTime(@Parameter( name="precision",optional = true) Integer precision) {
        if (precision == null) {
           return Duration.ofNanos(LocalTime.now().toNanoOfDay());
        }
        LocalTime now = LocalTime.now();
        int nano = now.getNano();
        String s = Integer.toString(nano);
        if (s.length()>precision){
            s = s.substring(0,precision);
            nano = Integer.parseInt(s);
        }
        return Duration.ofSeconds(now.toSecondOfDay()).plusNanos(nano);
    }
}
