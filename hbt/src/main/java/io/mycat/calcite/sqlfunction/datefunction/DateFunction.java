package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.mycat.MycatBuiltInMethodImpl;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.Temporal;

public class DateFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(DateFunction.class,
            "date");
    public static DateFunction INSTANCE = new DateFunction();

    public DateFunction() {
        super("date", scalarFunction);
    }

    public static LocalDate date(String s) {
        Temporal temporal = MycatBuiltInMethodImpl.timestampStringToTimestamp(s);
        if (temporal instanceof LocalDate) {
            return (LocalDate) temporal;
        }
        if (temporal instanceof LocalDateTime) {
            return ((LocalDateTime) temporal).toLocalDate();
        }
        throw new UnsupportedOperationException();
    }
}
