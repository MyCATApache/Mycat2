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


import io.mycat.calcite.sqlfunction.datefunction.SubTimeFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.function.TimeFunction;
import io.ordinate.engine.function.constant.PeriodConstant;
import io.ordinate.engine.record.Record;
import org.apache.calcite.mycat.MycatBuiltInMethodImpl;

import java.time.Duration;
import java.time.Period;
import java.util.List;

public class SubTimeFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "SUBTIME(string,string):string";
    }

    @Override
    public io.ordinate.engine.function.Function newInstance(List<io.ordinate.engine.function.Function> args, EngineConfiguration configuration) {
        return new Function(args);
    }

    class Function extends StringFunction {
        List<io.ordinate.engine.function.Function> args;
        boolean isNull;

        public Function(List<io.ordinate.engine.function.Function> args) {
            this.args = args;
        }

        @Override
        public List<io.ordinate.engine.function.Function> getArgs() {
            return args;
        }

        @Override
        public String getString(Record rec) {
            io.ordinate.engine.function.Function one = args.get(0);
            io.ordinate.engine.function.Function twoArg = args.get(1);
            CharSequence string1 = one.getString(rec);
            CharSequence string2 = twoArg.getString(rec);
            isNull = one.isNull(rec) || twoArg.isNull(rec);
            if (isNull) return null;
            return SubTimeFunction.subTime(string1.toString(), string2.toString());
        }
    }
}
