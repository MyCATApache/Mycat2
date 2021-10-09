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

package io.ordinate.engine.function.cast;

import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.constant.*;
import io.ordinate.engine.schema.InnerType;
import java.util.List;

public class CastNullFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(object)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        if (args.size() == 2) {
            InnerType type = args.get(1).getType();
            switch (type) {
                case BOOLEAN_TYPE:
                    return BooleanTypeConstant.INSTANCE;
                case INT8_TYPE:
                    return ByteTypeConstant.INSTANCE;
                case INT16_TYPE:
                    return ShortTypeConstant.INSTANCE;
                case INT32_TYPE:
                    return IntTypeConstant.INSTANCE;
                case INT64_TYPE:
                    return LongTypeConstant.INSTANCE;
                case FLOAT_TYPE:
                    return FloatTypeConstant.INSTANCE;
                case DOUBLE_TYPE:
                    return DoubleTypeConstant.INSTANCE;
                case STRING_TYPE:
                    return StringTypeConstant.INSTANCE;
                case BINARY_TYPE:
                    return BinaryTypeConstant.INSTANCE;
                case UINT8_TYPE:
                    return ByteTypeConstant.INSTANCE;
                case UINT16_TYPE:
                    return ShortTypeConstant.INSTANCE;
                case UINT32_TYPE:
                    return IntTypeConstant.INSTANCE;
                case UINT64_TYPE:
                    return LongTypeConstant.INSTANCE;
                case TIME_MILLI_TYPE:
                    return TimeTypeConstant.INSTANCE;
                case DATE_TYPE:
                    return DateTypeConstant.INSTANCE;
                case DATETIME_MILLI_TYPE:
                    return DatetimeTypeConstant.INSTANCE;
                case SYMBOL_TYPE:
                    return SymbolTypeConstant.INSTANCE;
            }
        }
        throw new UnsupportedOperationException();
    }

}
