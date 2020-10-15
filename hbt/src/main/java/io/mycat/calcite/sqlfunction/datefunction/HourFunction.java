package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.Duration;
import java.time.LocalDate;

public class HourFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(HourFunction.class,
            "hour");
    public static HourFunction INSTANCE = new HourFunction();

    public HourFunction() {
        super("HOUR", scalarFunction);
    }

    public static Long hour(Duration date) {
        if (date == null){
            return null;
        }
        return date.toHours();
    }
}

