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

package io.ordinate.engine.function.bind;

import io.ordinate.engine.function.BinarySequence;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.function.Function;
import org.apache.arrow.vector.*;
import io.ordinate.engine.physicalplan.CorrelateJoinPlan;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class VerSetterImpl implements CorrelateJoinPlan.VerSetter {
    final Map<Integer, List<VariableParameterFunction>> variableParameterFunctionMap;

    public VerSetterImpl(Map<Integer, List<VariableParameterFunction>> variableParameterFunctionMap) {
        this.variableParameterFunctionMap = variableParameterFunctionMap;
    }

    @Override
    public void set(int rowId, VectorSchemaRoot leftInput) {
        for (Map.Entry<Integer, List<VariableParameterFunction>> e : variableParameterFunctionMap.entrySet()) {
            Integer key = e.getKey();
            for (VariableParameterFunction value : e.getValue()) {
                Function base = value.getBase();

                FieldVector vector = leftInput.getVector(key);
                InnerType type = value.getType();
                switch (type) {
                    case BOOLEAN_TYPE: {
                        BooleanBindVariable booleanBindVariable = (BooleanBindVariable) base;
                        if (vector.isNull(rowId)) {
                            booleanBindVariable.isNull = true;
                        } else {
                            booleanBindVariable.isNull = false;
                            booleanBindVariable.value = ((BitVector) vector).get(rowId);
                        }

                        break;
                    }
                    case INT8_TYPE: {
                        ByteBindVariable byteBindVariable = (ByteBindVariable) base;
                        if (vector.isNull(rowId)) {
                            byteBindVariable.isNull = true;
                        } else {
                            byteBindVariable.isNull = false;
                            byteBindVariable.value = ((TinyIntVector) vector).get(rowId);
                        }
                        break;
                    }
                    case INT16_TYPE:
                    case CHAR_TYPE: {
                        ShortBindVariable shortBindVariable = (ShortBindVariable) base;
                        if (vector.isNull(rowId)) {
                            shortBindVariable.isNull = true;
                        } else {
                            shortBindVariable.isNull = false;
                            shortBindVariable.value = ((SmallIntVector) vector).get(rowId);
                        }
                        break;
                    }
                    case INT32_TYPE: {
                        IntBindVariable intBindVariable = (IntBindVariable) base;
                        if (vector.isNull(rowId)) {
                            intBindVariable.isNull = true;
                        } else {
                            intBindVariable.isNull = false;
                            intBindVariable.value = ((IntVector) vector).get(rowId);
                        }
                        break;
                    }
                    case INT64_TYPE: {
                        LongBindVariable longBindVariable = (LongBindVariable) base;
                        if (vector.isNull(rowId)) {
                            longBindVariable.isNull = true;
                        } else {
                            longBindVariable.isNull = false;
                            longBindVariable.value = ((BigIntVector) vector).get(rowId);
                        }
                        break;
                    }
                    case FLOAT_TYPE: {
                        FloatBindVariable floatBindVariable = (FloatBindVariable) base;
                        if (vector.isNull(rowId)) {
                            floatBindVariable.isNull = true;
                        } else {
                            floatBindVariable.isNull = false;
                            floatBindVariable.value = ((Float4Vector) vector).get(rowId);
                        }

                        break;
                    }
                    case DOUBLE_TYPE: {
                        DoubleBindVariable doubleBindVariable = (DoubleBindVariable) base;
                        if (vector.isNull(rowId)) {
                            doubleBindVariable.isNull = true;
                        } else {
                            doubleBindVariable.isNull = false;
                            doubleBindVariable.value = ((Float8Vector) vector).get(rowId);
                        }

                        break;
                    }
                    case SYMBOL_TYPE:
                    case STRING_TYPE: {
                        StringBindVariable strBindVariable = (StringBindVariable) base;
                        if (vector.isNull(rowId)) {
                            strBindVariable.isNull = true;
                        } else {
                            strBindVariable.isNull = false;
                            byte[] bytes = ((VarCharVector) vector).get(rowId);
                            strBindVariable.value = new String(bytes, StandardCharsets.UTF_8);
                        }
                        break;
                    }
                    case BINARY_TYPE: {
                        BinaryBindVariable binBindVariable = (BinaryBindVariable) base;
                        if (vector.isNull(rowId)) {
                            binBindVariable.isNull = true;
                        } else {
                            binBindVariable.isNull = false;
                            byte[] bytes = ((VarBinaryVector) vector).get(rowId);
                            binBindVariable.value = BinarySequence.of(bytes);
                        }
                        break;
                    }
                    case UINT8_TYPE:
                    case UINT16_TYPE:
                    case UINT32_TYPE:
                    case UINT64_TYPE:
                        throw new UnsupportedOperationException();
                    case TIME_MILLI_TYPE: {
                        TimeBindVariable timeBindVariable = (TimeBindVariable) base;
                        if (vector.isNull(rowId)) {
                            timeBindVariable.isNull = true;
                        } else {
                            timeBindVariable.isNull = false;
                            timeBindVariable.value = ((TimeMilliVector) vector).get(rowId);
                        }
                        break;
                    }
                    case DATE_TYPE: {
                        DateBindVariable dateBindVariable = (DateBindVariable) base;
                        if (vector.isNull(rowId)) {
                            dateBindVariable.isNull = true;
                        } else {
                            dateBindVariable.isNull = false;
                            dateBindVariable.value = ((DateMilliVector) vector).get(rowId);
                        }
                        break;
                    }
                    case DATETIME_MILLI_TYPE: {
                        TimestampBindVariable timestampBindVariable = (TimestampBindVariable) base;
                        if (vector.isNull(rowId)) {
                            timestampBindVariable.isNull = true;
                        } else {
                            timestampBindVariable.isNull = false;
                            timestampBindVariable.value = ((TimeStampVector) vector).get(rowId);
                        }
                        break;
                    }
                    case OBJECT_TYPE:
                    case NULL_TYPE:
                        throw new UnsupportedOperationException();
                    default:
                        throw new IllegalStateException("Unexpected value: " + value.getType());
                }
            }


        }

    }
}
