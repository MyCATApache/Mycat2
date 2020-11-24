package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.LocalDate;

public class DayOfYearFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(DayOfYearFunction.class,
            "dayOfYear");
    public static DayOfYearFunction INSTANCE = new DayOfYearFunction();

    public DayOfYearFunction() {
        super("DAYOFYEAR", scalarFunction);
    }

    public static Integer dayOfYear(LocalDate date) {
        if (date == null){
            return null;
        }
        return date.getDayOfYear();
    }
}
