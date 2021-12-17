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

import io.mycat.calcite.sqlfunction.datefunction.DayOfWeekFunction;
import io.mycat.calcite.sqlfunction.datefunction.FromDaysFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.*;
import io.ordinate.engine.record.Record;

import java.sql.Date;
import java.time.LocalDate;
import java.util.List;

public class FromDaysFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "FROM_DAYS(long):date";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args.get(0));
    }

    private static final class Func extends DateFunction implements UnaryFunction {

        private final Function arg;
        boolean isNull;

        public Func(Function arg) {
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
        public long getDate(Record rec) {
            long aLong = arg.getLong(rec);
            isNull = arg.isNull(rec);
            if (isNull) return 0;
            LocalDate localDate = FromDaysFunction.fromDays(aLong);
            return Date.valueOf(localDate).getTime();
        }
    }
}
