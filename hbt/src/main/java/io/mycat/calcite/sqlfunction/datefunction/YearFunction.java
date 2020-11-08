package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.LocalDate;
import java.time.temporal.WeekFields;
import java.util.Locale;

public class YearFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(YearFunction.class,
            "year");
    public static YearFunction INSTANCE = new YearFunction();

    public YearFunction() {
        super("YEAR", scalarFunction);
    }

    public static Integer year(LocalDate date) {
        if (date==null) {
            return null;
        }
       return date.getYear();
    }
}

