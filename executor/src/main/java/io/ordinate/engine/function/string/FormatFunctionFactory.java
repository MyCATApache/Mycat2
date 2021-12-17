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

package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.FormatFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.util.List;
import java.util.stream.Collectors;

public class FormatFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "format()";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args);
    }

    private static final class Func extends StringFunction {

        private final List<Function> args;
        private boolean isNull;

        public Func(List<Function> args) {
            super();
            this.args = args;
        }

        @Override
        public List<Function> getArgs() {
            return args;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }


        @Override
        public CharSequence getString(Record rec) {
            List<Object> collect = this.args.stream().map(i -> i.getAsObject(rec)).collect(Collectors.toList());
            String format = FormatFunction.format(collect.toArray(new Object[]{}));
            isNull = format == null;
            if (isNull) return null;
            return format;
        }
    }
}
