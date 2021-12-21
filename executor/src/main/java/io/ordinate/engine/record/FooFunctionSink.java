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

package io.ordinate.engine.record;

import io.ordinate.engine.function.BinarySequence;
import io.ordinate.engine.function.Function;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.util.Text;

public class FooFunctionSink implements FunctionSink{
    @Override
    public void copy(Function[] functions, Record inputRecord, int rowId, VectorSchemaRoot output) {
        int columnId = 0;
        for (Function function : functions) {

            FieldVector vector = output.getVector(columnId);
            switch (function.getType()) {
                case BOOLEAN_TYPE: {
                    int value = function.getInt(inputRecord);
                    boolean isNull = function.isNull(inputRecord);
                    BitVector bitVector = (BitVector) vector;
                    if (isNull) {
                        bitVector.setNull(rowId);
                    } else {
                        bitVector.set(rowId, value);
                    }
                    break;
                }
                case INT8_TYPE:{
                    int value = function.getByte(inputRecord);
                    boolean isNull = function.isNull(inputRecord);
                    setInt(rowId, output, columnId, value, isNull);
                    break;
                }
                case UINT8_TYPE: {
                    int value = function.getInt(inputRecord);
                    boolean isNull = function.isNull(inputRecord);
                    setInt(rowId, output, columnId, value, isNull);
                    break;
                }
                case CHAR_TYPE:{
                    int value = function.getInt(inputRecord);
                    boolean isNull = function.isNull(inputRecord);
                    setInt(rowId, output, columnId, value, isNull);
                    break;
                }
                case INT16_TYPE:{
                    int value = function.getInt(inputRecord);
                    boolean isNull = function.isNull(inputRecord);
                    setInt(rowId, output, columnId, value, isNull);
                    break;
                }
                case UINT16_TYPE: {
                    int value = function.getInt(inputRecord);
                    boolean isNull = function.isNull(inputRecord);
                    setInt(rowId, output, columnId, value, isNull);
                    break;
                }
                case INT32_TYPE: {
                    int value = function.getInt(inputRecord);
                    boolean isNull = function.isNull(inputRecord);
                    setInt(rowId, output, columnId, value, isNull);
                    break;
                }
                case UINT32_TYPE: {
                    int value = function.getInt(inputRecord);
                    boolean isNull = function.isNull(inputRecord);
                    setInt(rowId, output, columnId, value, isNull);
                    break;
                }
                case INT64_TYPE:{
                    long value = function.getLong(inputRecord);
                    boolean isNull = function.isNull(inputRecord);
                    BigIntVector bigIntVector = (BigIntVector) output.getVector(columnId);
                    setInt(rowId, output, columnId, value, isNull);
                    break;
                }
                case UINT64_TYPE: {
                    long value = function.getLong(inputRecord);
                    boolean isNull = function.isNull(inputRecord);
                    setInt(rowId, output, columnId, value, isNull);
                    break;
                }
                case FLOAT_TYPE: {
                    float value = function.getFloat(inputRecord);
                    boolean isNull = function.isNull(inputRecord);
                    Float4Vector float4Vector = (Float4Vector) output.getVector(columnId);
                    if (isNull) {
                        float4Vector.setNull(rowId);
                    } else {
                        float4Vector.set(rowId, value);
                    }
                    break;
                }
                case DOUBLE_TYPE: {
                    double value = function.getDouble(inputRecord);
                    boolean isNull = function.isNull(inputRecord);
                    Float8Vector float8Vector = (Float8Vector) output.getVector(columnId);
                    if (isNull) {
                        float8Vector.setNull(rowId);
                    } else {
                        float8Vector.set(rowId, value);
                    }
                    break;
                }
                case SYMBOL_TYPE:
                case STRING_TYPE: {
                    CharSequence value = function.getString(inputRecord);
                    boolean isNull = function.isNull(inputRecord);
                    VarCharVector varCharVector = (VarCharVector) output.getVector(columnId);
                    if (isNull) {
                        varCharVector.setNull(rowId);
                    } else {
                        varCharVector.set(rowId, new Text(value.toString()));
                    }
                    break;
                }
                case BINARY_TYPE: {
                    BinarySequence value = function.getBinary(inputRecord);
                    boolean isNull = function.isNull(inputRecord);
                    VarBinaryVector varBinaryVector = (VarBinaryVector) output.getVector(columnId);
                    if (isNull) {
                        varBinaryVector.setNull(rowId);
                    } else {
                        varBinaryVector.set(rowId, value.getBytes());
                    }
                    break;
                }
                case TIME_MILLI_TYPE: {
                    long value = function.getTime(inputRecord);
                    boolean isNull = function.isNull(inputRecord);
                    TimeMilliVector timeMilliVector = (TimeMilliVector) output.getVector(columnId);
                    if (isNull) {
                        timeMilliVector.setNull(rowId);
                    } else {
                        timeMilliVector.set(rowId, (int) value);
                    }
                    break;
                }
                case DATE_TYPE: {
                    long value = function.getDate(inputRecord);
                    boolean isNull = function.isNull(inputRecord);
                    DateMilliVector dateMilliVector = (DateMilliVector) output.getVector(columnId);
                    if (isNull) {
                        dateMilliVector.setNull(rowId);
                    } else {
                        dateMilliVector.set(rowId, value);
                    }
                    break;
                }
                case DATETIME_MILLI_TYPE: {
                    long value = function.getDatetime(inputRecord);
                    boolean isNull = function.isNull(inputRecord);
                    TimeStampVector dateMilliVector = (TimeStampVector) output.getVector(columnId);
                    if (isNull) {
                        dateMilliVector.setNull(rowId);
                    } else {
                        dateMilliVector.set(rowId, value);
                    }
                    break;
                }
                case OBJECT_TYPE:
                case NULL_TYPE:
                    throw new UnsupportedOperationException();
            }
            columnId++;
        }

    }

    private void setInt(int rowId, VectorSchemaRoot output, int columnId, long value, boolean isNull) {
        BaseIntVector outputVector = (BaseIntVector) output.getVector(columnId);
        if (isNull) {
            BaseFixedWidthVector baseFixedWidthVector = (BaseFixedWidthVector) outputVector;
            baseFixedWidthVector.setNull(rowId);
        } else {
            outputVector.setWithPossibleTruncate(rowId, value);
        }
    }
}
