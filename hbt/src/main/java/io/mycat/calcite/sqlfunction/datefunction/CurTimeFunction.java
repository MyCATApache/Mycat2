package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;

import java.time.Clock;
import java.time.Duration;
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

    public static Duration curTime() {
        return curTime(null);
    }

    public static Duration curTime(@Parameter(name = "precision", optional = true) Integer precision) {
        if (precision == null) {
            Duration duration = Duration.ofSeconds(LocalTime.now().toSecondOfDay());
            return duration;
        }
        LocalTime now = LocalTime.now();
        int nano = now.getNano();
        String s = Integer.toString(nano);
        if (s.length() > precision) {
            s = s.substring(0, precision);
            nano = Integer.parseInt(s);
        }
        return Duration.ofSeconds(now.toSecondOfDay()).plusNanos(nano);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        super.unparse(writer, call, leftPrec, rightPrec);
    }
}
