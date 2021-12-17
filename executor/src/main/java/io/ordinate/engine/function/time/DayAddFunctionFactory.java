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
import io.ordinate.engine.function.*;
import io.ordinate.engine.record.Record;
import io.ordinate.engine.schema.InnerType;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.datetime.millitime.Dates;

import java.util.List;

public class DayAddFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "DATE_ADD(date,time):date";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new DateAddFunction(args);
    }

    class DateAddFunction extends DateFunction {
        List<Function> args;
        boolean isNull;

        public DateAddFunction(List<Function> args) {
            this.args = args;
        }

        @Override
        public List<Function> getArgs() {
            return args;
        }

        @Override
        public long getDate(Record rec) {
            Function one = args.get(0);
            Function two = args.get(1);

            long date = one.getDate(rec);
            long time = two.getTime(rec);

            isNull = one.isNull(rec) || two.isNull(rec);
            if (isNull) return 0;
            long l = date + time;
            return l;
        }
    }

    ;
}
