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
