package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;

public class MonthFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(MonthFunction.class,
            "month");
    public static MonthFunction INSTANCE = new MonthFunction();

    public MonthFunction() {
        super("MONTH",
                scalarFunction
        );
    }

    public static int month(LocalDate localDate) {
        return localDate.getMonth().getValue();
    }
}
