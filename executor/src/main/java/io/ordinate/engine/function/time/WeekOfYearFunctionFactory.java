/*
 *     Copyright (C) <2021>  <Junwen Chen>
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package io.ordinate.engine.function.time;

import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.UnaryFunction;
import io.ordinate.engine.record.Record;
import io.questdb.std.datetime.microtime.Timestamps;

import java.time.LocalDate;
import java.time.temporal.WeekFields;
import java.util.List;

public class WeekOfYearFunctionFactory  implements FunctionFactory {
    @Override
    public String getSignature() {
        return "weekOfYear(date)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args.get(0));
    }
    private static final class Func extends IntFunction implements UnaryFunction {

        private final Function arg;
        private boolean isNull;

        public Func(Function arg) {
            super();
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }

        @Override
        public int getInt(Record rec) {
            final long value = arg.getDatetime(rec);
            isNull = arg.isNull(rec);
            if (isNull)return 0;

            int year = Timestamps.getYear(value);
            boolean leapYear = Timestamps.isLeapYear(year);
            int monthOfYear = Timestamps.getMonthOfYear(value, year,leapYear );
            int dayOfMonth = Timestamps.getDayOfMonth(value, year, monthOfYear, leapYear);
            LocalDate localDate = LocalDate.of(year, monthOfYear, dayOfMonth);
            return localDate.get(WeekFields.ISO.weekOfWeekBasedYear());
        }
    }
}
