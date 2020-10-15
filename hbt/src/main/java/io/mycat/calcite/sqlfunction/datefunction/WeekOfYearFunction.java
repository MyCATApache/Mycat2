package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.time.temporal.WeekFields;

public class WeekOfYearFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(WeekOfYearFunction.class,
            "weekOfYear");
    public static WeekOfYearFunction INSTANCE = new WeekOfYearFunction();

    public WeekOfYearFunction() {
        super("WEEKOFYEAR", scalarFunction);
    }

    public static Integer weekOfYear(LocalDate date) {
        if (date == null)return null;
        return date.get(WeekFields.ISO.weekOfWeekBasedYear());
    }
}

