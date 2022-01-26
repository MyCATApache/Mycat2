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

package io.ordinate.engine.function.bool;

import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.*;
import io.ordinate.engine.record.Record;

import java.util.List;

public class CaseLongFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "case(bool,int64,int64)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func((BooleanFunction) args.get(0), (ScalarFunction) args.get(1), (ScalarFunction) args.get(2));
    }


    private static class Func extends LongFunction {


        private final BooleanFunction condition;
        private final ScalarFunction first;
        private final ScalarFunction second;

        private boolean isNull;

        public Func(BooleanFunction condition, ScalarFunction first, ScalarFunction second) {
            this.condition = condition;
            this.first = first;
            this.second = second;
        }

        @Override
        public long getLong(Record rec) {
            if (condition.getBooleanType(rec)) {
                long aLong = first.getLong(rec);
                isNull = first.isNull(rec);
                return aLong;
            }
            long aLong = second.getLong(rec);
            isNull = second.isNull(rec);
            return aLong;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }
    }
}
