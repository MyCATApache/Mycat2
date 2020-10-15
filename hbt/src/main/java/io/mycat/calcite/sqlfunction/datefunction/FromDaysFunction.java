package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;

public class FromDaysFunction  extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(FromDaysFunction.class,
            "fromDays");
    public static FromDaysFunction INSTANCE = new FromDaysFunction();

    public FromDaysFunction() {
        super("FROM_DAYS", scalarFunction);
    }

    public static LocalDate fromDays(Long day) {
        if (day == null){
            return null;
        }
        return LocalDate.ofYearDay(1,1)
                .plusDays(day)
                .minusDays(1).minusYears(1);
    }
}

