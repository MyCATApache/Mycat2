/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.ordinate.engine.function.cast;

import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.record.Record;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.UnaryFunction;

import java.util.List;

public class CastBooleanToIntFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(bool):int32";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args.get(0));
    }


    private static class Func extends IntFunction implements UnaryFunction {
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
        public int getInt(Record rec) {
            int anInt = arg.getInt(rec);
            isNull = arg.isNull(rec);
            if (isNull)return 0;
            return anInt;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }
    }
}
