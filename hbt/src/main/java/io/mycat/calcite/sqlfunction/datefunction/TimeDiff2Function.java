package io.mycat.calcite.sqlfunction.datefunction;

import com.github.sisyphsu.dateparser.DateParserUtils;
import org.apache.calcite.mycat.MycatBuiltInMethodImpl;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.Temporal;
import java.util.Date;

public class TimeDiff2Function extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(TimeDiff2Function.class,
            "timdDiff");
    public static TimeDiff2Function INSTANCE = new TimeDiff2Function();

    public TimeDiff2Function() {
        super("TIMEDIFF", scalarFunction);
    }

    public static Duration timdDiff(String date1, String date2) {
        if (date1 == null||date2 == null){
            return null;
        }
        Object o = MycatBuiltInMethodImpl.parseUnknownDateTimeText(date1);
        Object o1 = MycatBuiltInMethodImpl.parseUnknownDateTimeText(date2);
        if (o instanceof Temporal&&o1 instanceof Temporal){
            return Duration.between((Temporal)o1,(Temporal)o);
        }
        if (o instanceof Duration&&o1 instanceof Duration){
            return ((Duration) o).minus((Duration)o1);
        }
        return null;
    }
}
