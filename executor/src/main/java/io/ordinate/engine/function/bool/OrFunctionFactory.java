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
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.BinaryArgFunction;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.record.Record;
import io.ordinate.engine.function.ScalarFunction;
import io.ordinate.engine.function.BooleanFunction;

import java.util.List;

public class OrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "or(bool,bool)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func((ScalarFunction) args.get(0), (ScalarFunction) args.get(1));
    }

    private static class Func extends BooleanFunction implements BinaryArgFunction {
        final ScalarFunction left;
        final ScalarFunction right;

        public Func(ScalarFunction left, ScalarFunction right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public int getInt(Record rec) {
            return (left.getInt(rec) > 1 || right.getInt(rec) > 1) ? 1 : 0;
        }

        @Override
        public ScalarFunction getLeft() {
            return left;
        }

        @Override
        public ScalarFunction getRight() {
            return right;
        }
    }
}
