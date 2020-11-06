package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.*;

public class DateDiffFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(DateDiffFunction.class,
            "dateDiff");
    public static DateDiffFunction INSTANCE = new DateDiffFunction();

    public DateDiffFunction() {
        super("DATEDIFF",
                scalarFunction
        );
    }

    public static Long dateDiff(LocalDate date0, LocalDate date1) {
        if (date0 == null || date1 == null) {
            return null;
        }
        long f = date0.toEpochDay();
        long f2 = date1.toEpochDay();
        return f - f2;
    }
}
