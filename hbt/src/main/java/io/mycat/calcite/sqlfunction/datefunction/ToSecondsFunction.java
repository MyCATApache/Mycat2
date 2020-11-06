package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

//unsupport SELECT TO_SECONDS(130513);
public class ToSecondsFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(ToSecondsFunction.class,
            "toSeconds");
    public static ToSecondsFunction INSTANCE = new ToSecondsFunction();

    public ToSecondsFunction() {
        super("TO_SECONDS",
                scalarFunction
        );
    }

    public static Long toSeconds(LocalDateTime date) {
        if (date==null) {
            return null;
        }
        //@todo
        LocalDateTime startDate = LocalDate.of(0, 1, 1).atStartOfDay();
        return (ChronoUnit.SECONDS.between(startDate, date));
    }
}
