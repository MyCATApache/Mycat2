package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.LocalDate;
import java.time.format.TextStyle;
import java.util.Locale;

public class MonthNameFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(MonthNameFunction.class,
            "monthName");
    public static MonthNameFunction INSTANCE = new MonthNameFunction();

    public MonthNameFunction() {
        super("MONTHNAME",
                scalarFunction
        );
    }

    public static String monthName(LocalDate localDate) {
        return localDate.getMonth().getDisplayName(TextStyle.FULL, Locale.US);
    }
}
