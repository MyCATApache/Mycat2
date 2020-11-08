package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.LocalDate;

public class DayOfWeekFunction  extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(DayOfWeekFunction.class,
            "dayOfWeek");
    public static DayOfWeekFunction INSTANCE = new DayOfWeekFunction();

    public DayOfWeekFunction() {
        super("DAYOFWEEK", scalarFunction);
    }

    public static Integer dayOfWeek(LocalDate date) {
        if (date == null){
            return null;
        }
        return date.getDayOfWeek().getValue()+1;
    }
}

