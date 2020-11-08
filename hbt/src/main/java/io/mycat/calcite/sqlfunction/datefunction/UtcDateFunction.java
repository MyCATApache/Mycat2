package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.sql.Timestamp;
import java.time.*;

public class UtcDateFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(UtcDateFunction.class,
            "utcDate");
    public static UtcDateFunction INSTANCE = new UtcDateFunction();

    public UtcDateFunction() {
        super("UTC_DATE", scalarFunction);
    }

    public static LocalDate utcDate() {
      return LocalDate.now(ZoneOffset.UTC);
    }
}

