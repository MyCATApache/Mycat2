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

package io.ordinate.engine.function.math;


import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.UnaryFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.record.Record;
import io.ordinate.engine.function.Function;

import java.util.List;

public class AbsIntFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "abs(int32)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new AbsIntFunction(args.get(0));
    }

    private static class AbsIntFunction extends IntFunction implements UnaryFunction {
        private final Function arg;
        boolean isNull;
        public AbsIntFunction(Function arg) {
            super();
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }


        @Override
        public int getInt(Record rec) {
            int value = arg.getInt(rec);
            isNull = arg.isNull(rec);
            return Math.abs(value);
        }

        @Override
        public boolean isNull(Record rec) {
            return super.isNull(rec);
        }
    }
}
