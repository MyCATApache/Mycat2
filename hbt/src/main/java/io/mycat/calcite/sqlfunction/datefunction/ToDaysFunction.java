package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.chrono.ChronoLocalDate;
import java.time.chrono.ChronoPeriod;
import java.time.temporal.ChronoUnit;

public class ToDaysFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(ToDaysFunction.class,
            "toDays");
    public static ToDaysFunction INSTANCE = new ToDaysFunction();

    public ToDaysFunction() {
        super("TO_DAYS",
                scalarFunction
        );
    }

    public static Long toDays(LocalDate date) {
        if (date==null) {
            return null;
        }
        //@todo
        LocalDate startDate = LocalDate.of(0, 1, 1);
        return (ChronoUnit.DAYS.between(startDate, date));
    }
}
