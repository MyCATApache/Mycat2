package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.LocalDate;

public class YearWeekFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(YearWeekFunction.class,
            "yearWeek");
    public static YearWeekFunction INSTANCE = new YearWeekFunction();

    public YearWeekFunction() {
        super("YEARWEEK", scalarFunction);
    }

    public static Integer yearWeek(LocalDate date) {
        return yearWeek(date,null);
    }

    public static Integer yearWeek(LocalDate date, @Parameter(name = "mode", optional = true) Integer mode) {
        if (date==null) {
            return null;
        }
       return date.getYear()*100+WeekFunction.week(date,mode);
    }
}

