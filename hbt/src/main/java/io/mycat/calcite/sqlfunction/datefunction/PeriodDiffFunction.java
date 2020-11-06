package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

public class PeriodDiffFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(PeriodDiffFunction.class,
            "periodDiff");
    public static PeriodDiffFunction INSTANCE = new PeriodDiffFunction();

    public PeriodDiffFunction() {
        super("PERIOD_DIFF",
                scalarFunction
        );
    }

    public static long periodDiff(Integer p1,Integer p2) {
        if (p1 == null||p2 == null){
            return 0;
        }
        return convertPeriodToMonth(p1) - convertPeriodToMonth(p2);
    }

    public static long convertPeriodToMonth(long period) {
        long a, b;
        if (period == 0)
            return 0L;
        if ((a = period / 100) < YY_PART_YEAR)
            a += 2000;
        else if (a < 100)
            a += 1900;
        b = period % 100;
        return a * 12 + b - 1;
    }
    public static final int YY_PART_YEAR = 70;
    public static long convertMonthToPeriod(long month) {
        long year;
        if (month == 0L)
            return 0L;
        if ((year = month / 12) < 100) {
            year += (year < YY_PART_YEAR) ? 2000 : 1900;
        }
        return year * 100 + month % 12 + 1;
    }
}
