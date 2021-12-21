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
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.function.TimeFunction;
import io.ordinate.engine.function.constant.PeriodConstant;
import io.ordinate.engine.record.Record;
import org.apache.calcite.mycat.MycatBuiltInMethodImpl;

import java.time.Duration;
import java.time.Period;
import java.util.List;

public class DateSubFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "DATE_SUB():string";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new DateSubFunction(args);
    }

    class DateSubFunction extends StringFunction {
        List<Function> args;
        boolean isNull;

        public DateSubFunction(List<Function> args) {
            this.args = args;
        }

        @Override
        public List<Function> getArgs() {
            return args;
        }

        @Override
        public String getString(Record rec) {
            Function one = args.get(0);
            Function twoArg = args.get(1);

            if (twoArg instanceof PeriodConstant) {


                PeriodConstant periodConstant = (PeriodConstant) twoArg;

                Period period = (Period)periodConstant.getAsObject(rec);
                CharSequence string = one.getString(rec);

                isNull = one.isNull(rec) || twoArg.isNull(rec);
                if (one instanceof StringFunction){

                   return MycatBuiltInMethodImpl.dateSubString(string.toString(), period);
                }
            }
            if (twoArg instanceof TimeFunction) {


                TimeFunction periodConstant = (TimeFunction) twoArg;

                long period = (long)periodConstant.getTime(rec);
                CharSequence string = one.getString(rec);

                isNull = one.isNull(rec) || twoArg.isNull(rec);
                if (one instanceof StringFunction){

                    return MycatBuiltInMethodImpl.dateSubString(string.toString(), Duration.ofMillis(period));
                }
            }

            throw new UnsupportedOperationException();

        }


    }

    ;
}
