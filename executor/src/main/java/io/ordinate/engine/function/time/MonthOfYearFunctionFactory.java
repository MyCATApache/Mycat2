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

import io.mycat.calcite.sqlfunction.datefunction.MonthFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.UnaryFunction;
import io.ordinate.engine.record.Record;
import io.questdb.std.datetime.microtime.Timestamps;

import java.sql.Date;
import java.time.LocalDate;
import java.util.List;

public class MonthOfYearFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "month(date):int32";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args.get(0));
    }

    private static final class Func extends IntFunction implements UnaryFunction {

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
        public int getInt(Record rec) {
            final long value = arg.getDatetime(rec);
            isNull = arg.isNull(rec);
            if (isNull) return 0;
            LocalDate localDate = new Date(value).toLocalDate();
            return MonthFunction.month(localDate);
        }
    }
}
