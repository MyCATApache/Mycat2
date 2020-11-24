package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.mycat.MycatBuiltInMethodImpl;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.tools.ant.taskdefs.Local;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.TextStyle;
import java.time.temporal.Temporal;
import java.util.Locale;

public class DaynameFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(DaynameFunction.class,
            "dayname");
    public static DaynameFunction INSTANCE = new DaynameFunction();

    public DaynameFunction() {
        super("DAYNAME", scalarFunction);
    }

    public static String dayname(LocalDate s) {
        if (s == null) {
            return null;
        }
        return s.getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.US);
    }
}
