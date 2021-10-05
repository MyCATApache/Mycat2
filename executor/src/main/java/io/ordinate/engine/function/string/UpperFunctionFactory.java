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

import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class UpperFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "upper(string)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args.get(0));
    }
    private static final class Func extends StringFunction  {

        private final Function arg;
        private boolean isNull;

        public Func(Function arg) {
            super();
            this.arg = arg;
        }

        @Override
        public List<Function> getArgs() {
            return Collections.singletonList(arg);
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }

        @Override
        public CharSequence getString(Record rec) {
            CharSequence value = arg.getString(rec);
            isNull = arg.isNull(rec);
            if (isNull)return null;
            return value.toString().toUpperCase(Locale.ROOT);
        }
    }
}
