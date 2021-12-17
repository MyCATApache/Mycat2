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

import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.schema.IntInnerType;
import org.apache.arrow.vector.*;

import static io.ordinate.engine.record.impl.RecordSinkFactoryImpl.*;

public class FooRecordSink implements RecordSink {
    final IntInnerType[] types;

    public FooRecordSink(IntInnerType[] types) {
        this.types = types;
    }

    @Override
    public void copy(Record r, RecordSetter w) {
        for (IntInnerType intPair : types) {
            int i = intPair.index;
            boolean aNull = r.isNull(i);
            if (aNull) {
                copyNullType(r, w, i);
                continue;
            }
            InnerType innerType = intPair.type;
            switch (innerType) {
                case BOOLEAN_TYPE:
                    copyBooleanType(r, w, i);
                    break;
                case INT8_TYPE:
                    copyInt8Type(r, w, i);
                    break;
                case INT16_TYPE:
                    copyInt16Type(r, w, i);
                    break;
                case CHAR_TYPE:
                    copyCharType(r, w, i);
                    break;
                case INT32_TYPE:
                    copyInt32Type(r, w, i);
                    break;
                case INT64_TYPE:
                    copyInt64Type(r, w, i);
                    break;
                case FLOAT_TYPE:
                    copyFloatType(r, w, i);
                    break;
                case DOUBLE_TYPE:
                    copyDoubleType(r, w, i);
                    break;
                case STRING_TYPE:
                    copyStringType(r, w, i);
                    break;
                case BINARY_TYPE:
                    copyBinaryType(r, w, i);
                    break;
                case UINT8_TYPE:
                    copyUInt8Type(r, w, i);
                    break;
                case UINT16_TYPE:
                    copyUInt16Type(r, w, i);
                    break;
                case UINT32_TYPE:
                    copyUInt32Type(r, w, i);
                    break;
                case UINT64_TYPE:
                    copyUInt64Type(r, w, i);
                    break;
                case TIME_MILLI_TYPE:
                    copyTimeMillType(r, w, i);
                    break;
                case DATE_TYPE:
                    copyDateType(r, w, i);
                    break;
                case DATETIME_MILLI_TYPE:
                    copyDatetimeMilliType(r, w, i);
                    break;
                case SYMBOL_TYPE:
                    copySymbolType(r, w, i);
                    break;
                case OBJECT_TYPE:
                    copyObjectType(r, w, i);
                    break;
                case NULL_TYPE:
                    copyNullType(r, w, i);
                    break;
            }
        }
    }

    @Override
    public void copy(Record record, int rowId, VectorSchemaRoot input) {
        for (IntInnerType intPair : types) {
            int columnIndex = intPair.index;
            FieldVector vector = input.getVector(columnIndex);
            boolean aNull = record.isNull(columnIndex);
            if (aNull) {
                if (vector instanceof BaseFixedWidthVector) {
                    ((BaseFixedWidthVector) vector).setNull(rowId);
                } else if (vector instanceof BaseVariableWidthVector) {
                    ((BaseVariableWidthVector) vector).setNull(rowId);
                }
                continue;
            }
            InnerType innerType = intPair.type;
            switch (innerType) {
                case BOOLEAN_TYPE:
                    BitVector bitVector = (BitVector) vector;
                    bitVector.set(rowId, record.getInt(columnIndex));
                    break;
                case CHAR_TYPE:
                case INT16_TYPE:
                case INT8_TYPE:
                case INT32_TYPE:
                case INT64_TYPE:
                case UINT8_TYPE:
                case UINT16_TYPE:
                case UINT32_TYPE:
                case UINT64_TYPE:
                    BaseIntVector intVectors = (BaseIntVector) vector;
                    intVectors.setUnsafeWithPossibleTruncate(rowId, record.getLong(columnIndex));
                    break;
                case DOUBLE_TYPE:
                case FLOAT_TYPE:
                    FloatingPointVector vectors = (FloatingPointVector) vector;
                    vectors.setWithPossibleTruncate(rowId, record.getDouble(columnIndex));
                    break;
                case SYMBOL_TYPE:
                case STRING_TYPE:
                    VarCharVector valueVectors = (VarCharVector) vector;
                    valueVectors.set(rowId, record.getBinary(columnIndex).getBytes());
                    break;
                case BINARY_TYPE: {
                    VarBinaryVector varBinaryVector = (VarBinaryVector) vector;
                    varBinaryVector.set(rowId, record.getBinary(columnIndex).getBytes());
                    break;
                }
                case TIME_MILLI_TYPE: {
                    TimeMilliVector timeStampVector = (TimeMilliVector) vector;
                    timeStampVector.set(rowId, (int) record.getTime(columnIndex));
                    break;
                }

                case DATE_TYPE: {
                    DateMilliVector dateDayVector = (DateMilliVector) vector;
                    dateDayVector.set(rowId, record.getDate(columnIndex));
                    break;
                }
                case DATETIME_MILLI_TYPE: {
                    TimeStampVector timeStampVector = (TimeStampVector) vector;
                    timeStampVector.set(rowId, record.getTime(columnIndex));
                    break;
                }
                case OBJECT_TYPE:
                    throw new UnsupportedOperationException();
                case NULL_TYPE:
                    if (vector instanceof BaseFixedWidthVector) {
                        ((BaseFixedWidthVector) vector).setNull(rowId);
                    } else if (vector instanceof BaseVariableWidthVector) {
                        ((BaseVariableWidthVector) vector).setNull(rowId);
                    }
                    continue;
            }
        }
    }

}
