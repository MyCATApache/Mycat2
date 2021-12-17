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

import io.mycat.calcite.sqlfunction.stringfunction.FieldFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.util.List;
import java.util.stream.Collectors;

public class FieldFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "field()";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args);
    }

    private static final class Func extends IntFunction {

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
        public int getInt(Record rec) {
            boolean isNumber = this.args.get(0) instanceof IntFunction;
            if (isNumber){
                List<Integer> numbers = this.args.stream().map(i -> (IntFunction) i).map(i -> i.getInt(rec)).collect(Collectors.toList());
                Integer field = FieldFunction.field(numbers.toArray(new String[]{}));
                isNull = field == null;
                return field;
            }
            List<String> numbers = this.args.stream().map(i -> (StringFunction) i).map(i -> i.getString(rec).toString()).collect(Collectors.toList());
            Integer field = FieldFunction.field(numbers.toArray(new String[]{}));
            isNull = field == null;
            return field;
        }
//
//        @Override
//        public CharSequence getString(Record rec) {
//
//            Function delimiterFunction = this.args.get(0);
//            String delimiter = delimiterFunction.getString(rec).toString();
//            List<Function> args = this.args.subList(1, this.args.size());
//            CharSequence[] strings = new String[args.size()];
//
//            int index = 0;
//            for (Function arg : args) {
//                strings[ index]=arg.getString(rec);
//                 isNull = arg.isNull(rec);
//                if (isNull){
//                    return null;
//                }
//                index++;
//            }
//            return String.join(delimiter,strings);
//        }
    }
}
