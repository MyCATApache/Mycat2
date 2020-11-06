package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.LocalDate;
import java.time.temporal.WeekFields;
import java.util.Locale;

public class WeekDayFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(WeekDayFunction.class,
            "weekDay");
    public static WeekDayFunction INSTANCE = new WeekDayFunction();

    public WeekDayFunction() {
        super("WEEKDAY", scalarFunction);
    }

    public static Integer weekDay(LocalDate date) {
        if (date==null) {
            return null;
        }
        return date.getDayOfWeek().getValue()-1;
    }
}

