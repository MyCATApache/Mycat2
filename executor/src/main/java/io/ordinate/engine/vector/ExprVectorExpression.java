
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


package io.ordinate.engine.vector;

import io.ordinate.engine.function.BinarySequence;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.column.ColumnFunction;
import io.ordinate.engine.record.Record;
import lombok.Getter;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;

@Getter
public class ExprVectorExpression extends AbstractVectorExpression {

    private Function function;


    public ExprVectorExpression(Function function) {
        super(function.getType().getArrowType());
        this.function = function;
    }

    @Override
    public void eval(VectorContext ctx) {
        FieldVector outputVector = ctx.getOutputVector();
        int nullCount = outputVector.getNullCount();
        int rowCount = ctx.getRowCount();
        boolean nullable = function.isNullableConstant();
        Record record = function.generateSink(ctx);
        switch (function.getType()) {
            case BOOLEAN_TYPE: {
                BitVector bitVector = (BitVector) outputVector;
                if (function.isNullableConstant()) {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        int value = function.getInt(record) ;
                        boolean isNull = function.isNull(record);
                        if (isNull) {
                            bitVector.setNull(i);
                        } else {
                            bitVector.set(i, value);
                        }
                    }
                } else {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        int bool = function.getInt(record);
                        bitVector.set(i, bool);
                    }
                }
                break;
            }
            case INT8_TYPE:
                break;
            case INT16_TYPE:
                break;
            case INT32_TYPE:
            {
                IntVector valueVector = (IntVector) outputVector;
                if (function.isNullableConstant()) {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        int value = function.getInt(record);
                        if ( function.isNull(record)) {
                            valueVector.setNull(i);
                        } else {

                            valueVector.set(i, value);
                        }
                    }
                } else {
                    for (int i = 0; i < rowCount; i++) {
                        int value = function.getInt(record);
                        valueVector.set(i, value);
                    }
                }
                break;
            }
            case INT64_TYPE: {
                BigIntVector valueVector = (BigIntVector) outputVector;
                if (function.isNullableConstant()) {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        long value = function.getLong(record);
                        boolean isNull = function.isNull(record);
                        if (isNull) {
                            valueVector.setNull(i);
                        } else {
                            valueVector.set(i, value);
                        }
                    }
                } else {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        long value = function.getLong(record);
                        valueVector.set(i, value);
                    }
                }
                break;
            }
            case FLOAT_TYPE:
            {
                Float4Vector valueVector = (Float4Vector) outputVector;
                if (function.isNullableConstant()) {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        float value = function.getFloat(record);
                        boolean isNull = function.isNull(record);
                        if (isNull) {
                            valueVector.setNull(i);
                        } else {
                            valueVector.set(i, value);
                        }
                    }
                } else {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        float value = function.getFloat(record);
                        valueVector.set(i, value);
                    }
                }
                break;
            }
            case DOUBLE_TYPE:
            {
                Float8Vector valueVector = (Float8Vector) outputVector;
                if (function.isNullableConstant()) {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        double value = function.getDouble(record);
                        boolean isNull = function.isNull(record);
                        if (isNull) {
                            valueVector.setNull(i);
                        } else {
                            valueVector.set(i, value);
                        }
                    }
                } else {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        double value = function.getDouble(record);
                        valueVector.set(i, value);
                    }
                }
                break;
            }
            case STRING_TYPE:
            {
                VarCharVector valueVector = (VarCharVector) outputVector;
                if (function.isNullableConstant()) {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        CharSequence value = function.getString(record);
                        boolean isNull = function.isNull(record);
                        if (isNull) {
                            valueVector.setNull(i);
                        } else {
                            valueVector.setSafe(i, (value.toString().getBytes()));
                        }
                    }
                } else {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        CharSequence value = function.getString(record);
                        valueVector.set(i, value.toString().getBytes());
                    }
                }
                break;
            }
            case BINARY_TYPE:
            {
                VarBinaryVector valueVector = (VarBinaryVector) outputVector;
                if (function.isNullableConstant()) {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        BinarySequence value = function.getBinary(record);
                        boolean isNull = function.isNull(record);
                        if (isNull) {
                            valueVector.setNull(i);
                        } else {
                            valueVector.setSafe(i, (value.getBytes()));
                        }
                    }
                } else {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        BinarySequence value = function.getBinary(record);
                        valueVector.set(i, value.getBytes());
                    }
                }
                break;
            }
            case UINT8_TYPE:
                break;
            case UINT16_TYPE:
                break;
            case UINT32_TYPE:
                break;
            case UINT64_TYPE:
                break;
            case TIME_MILLI_TYPE:
            {
                TimeMilliVector valueVector = (TimeMilliVector) outputVector;
                if (function.isNullableConstant()) {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        int value = (int)function.getTime(record);
                        boolean isNull = function.isNull(record);
                        if (isNull) {
                            valueVector.setNull(i);
                        } else {
                            valueVector.set(i, (value));
                        }
                    }
                } else {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        int value = (int)function.getTime(record);
                        valueVector.set(i, value);
                    }
                }
                break;
            }
            case DATE_TYPE:
            {
                DateMilliVector valueVector = (DateMilliVector) outputVector;
                if (function.isNullableConstant()) {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        long value = function.getDate(record);
                        boolean isNull = function.isNull(record);
                        if (isNull) {
                            valueVector.setNull(i);
                        } else {
                            valueVector.set(i, value);
                        }
                    }
                } else {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        long value = function.getDate(record);
                        valueVector.set(i, value);
                    }
                }
                break;
            }
            case DATETIME_MILLI_TYPE:
            {
                TimeStampMilliVector valueVector = (TimeStampMilliVector) outputVector;
                if (function.isNullableConstant()) {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        long value = function.getDatetime(record);
                        boolean isNull = function.isNull(record);
                        if (isNull) {
                            valueVector.setNull(i);
                        } else {
                            valueVector.setSafe(i, (value));
                        }
                    }
                } else {
                    for (int i = 0; i < rowCount; i++) {
                        record.setPosition(i);
                        long value = function.getDatetime(record);
                        valueVector.set(i, value);
                    }
                }
                break;
            }
        }
    }

    @Override
    public String signature() {
        return null;
    }

    @Override
    public List<ArrowType> argTypes() {
        return null;
    }

    @Override
    public boolean isColumn() {
        return function instanceof ColumnFunction;
    }
}
