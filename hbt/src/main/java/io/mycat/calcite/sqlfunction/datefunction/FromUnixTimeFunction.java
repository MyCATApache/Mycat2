package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

public class FromUnixTimeFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(FromUnixTimeFunction.class,
            "fromUnixTime");
    public static FromUnixTimeFunction INSTANCE = new FromUnixTimeFunction();

    public FromUnixTimeFunction() {
        super("FROM_UNIXTIME", scalarFunction);
    }

    public static LocalDateTime fromUnixTime(long unix_timestamp) {
        Date date = new Date(unix_timestamp * 1000);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return localDateTime;
    }
}

