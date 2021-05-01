/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.LocalDate;
import java.time.Period;

public class PeriodAddFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(PeriodAddFunction.class,
            "periodAdd");
    public static PeriodAddFunction INSTANCE = new PeriodAddFunction();

    public PeriodAddFunction() {
        super("PERIOD_ADD",
                scalarFunction
        );
    }

    public static long periodAdd(Integer p,Integer n) {
        if (p == null||n == null){
            return 0;
        }
       return convertMonthToPeriod(convertPeriodToMonth(p) + n);
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
