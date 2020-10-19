package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class CurTimeFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(CurTimeFunction.class,
            "curTime");
    public static CurTimeFunction INSTANCE = new CurTimeFunction();

    public CurTimeFunction() {
        super("CURTIME",
                scalarFunction
        );
    }

    public static Duration curTime(@Parameter( name="precision",optional = true) Integer precision) {
        if (precision == null) {
           return Duration.ofNanos(LocalTime.now().toNanoOfDay());
        }
        LocalTime now = LocalTime.now();
        int nano = now.getNano();
        String s = Integer.toString(nano);
        if (s.length()>precision){
            s = s.substring(0,precision);
            nano = Integer.parseInt(s);
        }
        return Duration.ofSeconds(now.toSecondOfDay()).plusNanos(nano);
    }
}
