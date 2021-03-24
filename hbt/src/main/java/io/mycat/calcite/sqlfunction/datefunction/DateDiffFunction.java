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

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.*;

public class DateDiffFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(DateDiffFunction.class,
            "dateDiff");
    public static DateDiffFunction INSTANCE = new DateDiffFunction();

    public DateDiffFunction() {
        super("DATEDIFF",
                scalarFunction
        );
    }

    public static Long dateDiff(LocalDate date0, LocalDate date1) {
        if (date0 == null || date1 == null) {
            return null;
        }
        long f = date0.toEpochDay();
        long f2 = date1.toEpochDay();
        return f - f2;
    }
}
