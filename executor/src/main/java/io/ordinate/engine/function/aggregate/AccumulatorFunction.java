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

package io.ordinate.engine.function.aggregate;

import io.ordinate.engine.vector.AggregateVectorExpression;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.record.Record;
import io.ordinate.engine.function.Function;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;

import java.util.Optional;

public interface AccumulatorFunction<T> extends Function {

    String name();

    void computeFirst(MapValue reduceContext, Record record);

    void computeNext(MapValue resultValue, Record record);

    default public void allocContext(InnerType[] columnTypes) {
        ArrayColumnTypes arrayColumnTypes = new ArrayColumnTypes();
        for (int i = 0; i < columnTypes.length; i++) {
            InnerType columnType = columnTypes[i];
            switch (columnType) {
                case BOOLEAN_TYPE:
                    arrayColumnTypes.add(ColumnType.BOOLEAN);
                    break;
                case INT8_TYPE:
                    arrayColumnTypes.add(ColumnType.INT);
                    break;
                case INT16_TYPE:
                    arrayColumnTypes.add(ColumnType.SHORT);
                    break;
                case CHAR_TYPE:
                    arrayColumnTypes.add(ColumnType.CHAR);
                    break;
                case INT32_TYPE:
                    arrayColumnTypes.add(ColumnType.INT);
                    break;
                case INT64_TYPE:
                    arrayColumnTypes.add(ColumnType.LONG);
                    break;
                case FLOAT_TYPE:
                    arrayColumnTypes.add(ColumnType.FLOAT);
                    break;
                case DOUBLE_TYPE:
                    arrayColumnTypes.add(ColumnType.DOUBLE);
                    break;
                case STRING_TYPE:
                    arrayColumnTypes.add(ColumnType.STRING);
                    break;
                case BINARY_TYPE:
                    arrayColumnTypes.add(ColumnType.BINARY);
                    break;
                case UINT8_TYPE:
                    arrayColumnTypes.add(ColumnType.BYTE);
                    break;
                case UINT16_TYPE:
                    arrayColumnTypes.add(ColumnType.SHORT);
                    break;
                case UINT32_TYPE:
                    arrayColumnTypes.add(ColumnType.INT);
                    break;
                case UINT64_TYPE:
                    arrayColumnTypes.add(ColumnType.LONG);
                    break;
                case TIME_MILLI_TYPE:
                    arrayColumnTypes.add(ColumnType.LONG);
                    break;
                case DATE_TYPE:
                    arrayColumnTypes.add(ColumnType.DATE);
                    break;
                case DATETIME_MILLI_TYPE:
                    arrayColumnTypes.add(ColumnType.TIMESTAMP);
                    break;
                case SYMBOL_TYPE:
                case OBJECT_TYPE:
                case NULL_TYPE:
                default:
                    throw new IllegalStateException("Unexpected value: " + columnType);
            }
        }
        allocContext(arrayColumnTypes);
    }

    public void allocContext(ArrayColumnTypes columnTypes);

    default boolean isScalar() {
        return true;
    }

    public int getInputColumnIndex();

    InnerType getOutputType();

    InnerType getInputType();

    default InnerType getType(){
        return getOutputType();
    }

    default AggregateVectorExpression toAggregateVectorExpression() {
        return null;
    }

    default boolean isVector() {
        return false;
    }

    void setInputColumnIndex(int index);
}
