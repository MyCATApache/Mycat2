package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.mycat.MycatBuiltInMethodImpl;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Locale;

public class FromUnixTimeFormatFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(FromUnixTimeFormatFunction.class,
            "fromUnixTime");
    public static FromUnixTimeFormatFunction INSTANCE = new FromUnixTimeFormatFunction();

    public FromUnixTimeFormatFunction() {
        super("FROM_UNIXTIME", scalarFunction);
    }

    public static String fromUnixTime(long unix_timestamp,String format) {
        LocalDateTime localDateTime = FromUnixTimeFunction.fromUnixTime(unix_timestamp);
        return DateFormatFunction.dateFormat(MycatBuiltInMethodImpl.timestampToString(localDateTime),format,Locale.getDefault().toString());
    }
}

