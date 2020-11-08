package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.mycat.MycatBuiltInMethodImpl;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.Temporal;

public class DayOfMonthFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(DayOfMonthFunction.class,
            "dayOfMonth");
    public static DayOfMonthFunction INSTANCE = new DayOfMonthFunction();

    public DayOfMonthFunction() {
        super("DAYOFMONTH", scalarFunction);
    }

    public static Integer dayOfMonth(LocalDate date) {
        if (date == null){
            return null;
        }
        return date.getDayOfMonth();
    }
}
