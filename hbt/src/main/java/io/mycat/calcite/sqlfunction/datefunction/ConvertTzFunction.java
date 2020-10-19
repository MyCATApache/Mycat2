package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.LocalDateTime;

public class ConvertTzFunction  extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(ConvertTzFunction.class,
            "convertTz");
    public static ConvertTzFunction INSTANCE = new ConvertTzFunction();

    public ConvertTzFunction() {
        super("CONVERT_TZ", scalarFunction);
    }

    public static LocalDateTime convertTz(String dt,String from_tz,String to_tz){
        throw new UnsupportedOperationException();
    }
}
