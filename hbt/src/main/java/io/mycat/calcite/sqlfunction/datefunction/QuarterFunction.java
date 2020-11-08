package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;

public class QuarterFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(QuarterFunction.class,
            "quarter");
    public static QuarterFunction INSTANCE = new QuarterFunction();

    public QuarterFunction() {
        super("QUARTER",
                scalarFunction
        );
    }

    public static Integer quarter(LocalDate date) {
        if (date == null) {
            return null;
        }
        return date.get(IsoFields.QUARTER_OF_YEAR);
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
