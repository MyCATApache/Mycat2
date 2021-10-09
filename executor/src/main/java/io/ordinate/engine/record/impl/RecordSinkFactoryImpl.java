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

package io.ordinate.engine.record.impl;

import com.google.common.primitives.UnsignedLong;
import io.ordinate.engine.builder.SortOptions;
import io.ordinate.engine.function.BinarySequence;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.record.*;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.schema.IntInnerType;
import io.questdb.cairo.map.MapKey;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.util.Text;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;

public class RecordSinkFactoryImpl implements RecordSinkFactory {
    public  static  final RecordSinkFactoryImpl INSTANCE =  new RecordSinkFactoryImpl();

    public RecordSink buildRecordSink(IntInnerType[] types) {
        return new FooRecordSink(types);
    }

    @Override
    public FunctionSink buildFunctionSink(Function[] functions) {
        return new FooFunctionSink();
    }

    public static void copyNullType(Record r, RecordSetter w, int i) {
        w.setNull();
    }

    public static void copyObjectType(Record r, RecordSetter w, int i) {
        w.putObject(r.getObject(i));
    }

    public static void copySymbolType(Record r, RecordSetter w, int i) {
        w.putSymbol(r.getSymbol(i));
    }

    public static void copyDatetimeMilliType(Record r, RecordSetter w, int i) {
        w.putDatetime(r.getDatetime(i));
    }

    public static void copyDateType(Record r, RecordSetter w, int i) {
        w.putDate(r.getDate(i));
    }

    public static void copyTimeMillType(Record r, RecordSetter w, int i) {
        w.putTime(r.getTime(i));
    }

    public static void copyUInt64Type(Record r, RecordSetter w, int i) {
        w.putUInt64(r.getUInt64(i));
    }

    public static void copyUInt32Type(Record r, RecordSetter w, int i) {
        w.putUInt32(r.getUInt32(i));
    }

    public static void copyUInt16Type(Record r, RecordSetter w, int i) {
        w.putUInt16(r.getUInt16(i));
    }

    public static void copyStringType(Record r, RecordSetter w, int i) {
        w.putString(r.getString(i));
    }

    public static void copyBinaryType(Record r, RecordSetter w, int i) {
        w.putBinary(r.getBinary(i));
    }

    public static void copyDoubleType(Record r, RecordSetter w, int i) {
        w.putDouble(r.getDouble(i));
    }

    public static void copyFloatType(Record r, RecordSetter w, int i) {
        w.putFloat(r.getFloat(i));
    }

    public static void copyInt64Type(Record r, RecordSetter w, int i) {
        w.putLong(r.getLong(i));
    }

    public static void copyInt32Type(Record r, RecordSetter w, int i) {
        w.putInt(r.getInt(i));
    }

    public static void copyCharType(Record r, RecordSetter w, int i) {
        w.putChar(r.getChar(i));
    }

    public static void copyInt16Type(Record r, RecordSetter w, int i) {
        w.putShort(r.getShort(i));
    }

    public static void copyInt8Type(Record r, RecordSetter w, int i) {
        w.putByte(r.getByte(i));
    }

    public static void copyUInt8Type(Record r, RecordSetter w, int i) {
        w.putByte(r.getByte(i));
    }

    public static void copyBooleanType(Record r, RecordSetter w, int i) {
        w.putBool(r.getInt(i) > 0);
    }


    @NotNull
    public RecordSetter getRecordSinkSPI(Object o) {
        if (o instanceof MapKey) {
            MapKey mapKey = (MapKey) o;
            return new RecordSetter() {
                @Override
                public void setNull() {
                    mapKey.putBin(null);
                }

                @Override
                public void putBinary(BinarySequence value) {
                    mapKey.putBin(new io.questdb.std.BinarySequence() {
                        @Override
                        public byte byteAt(long index) {
                            return value.byteAt(index);
                        }

                        @Override
                        public long length() {
                            return value.length();
                        }
                    });
                }

                @Override
                public void putBool(boolean value) {
                    mapKey.putBool(value);
                }

                @Override
                public void putByte(byte value) {
                    mapKey.putByte(value);
                }

                @Override
                public void putDate(long value) {
                    mapKey.putDate(value);
                }

                @Override
                public void putDouble(double value) {
                    mapKey.putDouble(value);
                }

                @Override
                public void putFloat(float value) {
                    mapKey.putFloat(value);
                }

                @Override
                public void putInt(int value) {
                    mapKey.putInt(value);
                }

                @Override
                public void putLong(long value) {
                    mapKey.putLong(value);
                }

                @Override
                public void putShort(short value) {
                    mapKey.putShort(value);
                }

                @Override
                public void putChar(char value) {
                    mapKey.putChar(value);
                }

                @Override
                public void putString(CharSequence value) {
                    mapKey.putStr(value);
                }

                @Override
                public void putDatetime(long value) {
                    mapKey.putTimestamp(value);
                }

                @Override
                public void putTime(long time) {
                    mapKey.putTimestamp(time);
                }

                @Override
                public void putUInt16(short uInt16) {
                    putShort(uInt16);
                }

                @Override
                public void putUInt64(long uInt64) {
                    putLong(uInt64);
                }

                @Override
                public void putSymbol(CharSequence symbol) {
                    putString(symbol);
                }

                @Override
                public void putObject(Object object) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void putUInt32(int uInt32) {
                    putInt(uInt32);
                }
            };
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public RecordComparator buildEqualComparator(InnerType[] types) {
        return new RecordComparator() {
            Record leftRecord;
            Record rightRecord;
            @Override
            public int compare(Record record) {
                return compare(leftRecord,record);
            }

            @Override
            public int compare() {
                return compare(leftRecord,rightRecord);
            }

            @Override
            public void setLeft(Record record) {
                this.leftRecord = record;
            }

            @Override
            public void setRight(Record record) {
                this.rightRecord =record;
            }
            public int compare(Record left, Record right) {
                int length = types.length;
                int res=0;
                for (int columnIndex = 0; columnIndex < length; columnIndex++) {
                    InnerType type = types[columnIndex];


                    switch (type) {
                        case BOOLEAN_TYPE: {
                            res = Boolean.compare(left.getBooleanType(columnIndex) , right.getBooleanType(columnIndex));
                            break;
                        }
                        case INT8_TYPE: {
                            res = Integer.compare(left.getInt(columnIndex), right.getInt(columnIndex));
                            break;
                        }
                        case INT16_TYPE: {
                            res = Short.compare(left.getShort(columnIndex), right.getShort(columnIndex));
                            break;
                        }
                        case CHAR_TYPE: {
                            res = Character.compare(left.getCharType(columnIndex), right.getChar(columnIndex));
                            break;
                        }
                        case INT32_TYPE: {
                            res = Integer.compare(left.getInt32Type(columnIndex), right.getInt32Type(columnIndex));
                            break;
                        }
                        case INT64_TYPE: {
                            res = Long.compare(left.getInt64Type(columnIndex), right.getInt64Type(columnIndex));
                            break;
                        }
                        case FLOAT_TYPE: {
                            res = Float.compare(left.getFloatType(columnIndex), right.getFloatType(columnIndex));
                            break;
                        }
                        case DOUBLE_TYPE: {
                            res = Double.compare(left.getDoubleType(columnIndex), right.getDoubleType(columnIndex));
                            break;
                        }
                        case STRING_TYPE: {
                            res = left.getString(columnIndex).toString().compareTo(right.getString(columnIndex).toString());
                            break;
                        }
                        case BINARY_TYPE: {
                            BinarySequence leftBinary = left.getBinary(columnIndex);
                            BinarySequence rightBinary = right.getBinary(columnIndex);
                            res = leftBinary.compareTo(rightBinary);
                            break;
                        }
                        case UINT8_TYPE: {
                            int l = Byte.toUnsignedInt(left.getUInt8Type(columnIndex));
                            int r = Byte.toUnsignedInt(right.getUInt8Type(columnIndex));
                            res = Integer.compare(l, r);
                            break;
                        }
                        case UINT16_TYPE: {
                            int l = Short.toUnsignedInt(left.getUInt16(columnIndex));
                            int r = Short.toUnsignedInt(right.getUInt16(columnIndex));
                            res = Integer.compare(l, r);
                            break;
                        }
                        case UINT32_TYPE: {
                            long l = Integer.toUnsignedLong(left.getUInt32(columnIndex));
                            long r = Integer.toUnsignedLong(right.getUInt32(columnIndex));
                            res = Long.compare(l, r);
                            break;
                        }
                        case UINT64_TYPE: {
                            res = UnsignedLong.fromLongBits(left.getUInt64(columnIndex)).compareTo(UnsignedLong.fromLongBits(right.getUInt64(columnIndex)));
                            break;
                        }
                        case TIME_MILLI_TYPE: {
                            res = UnsignedLong.fromLongBits(left.getTime(columnIndex)).compareTo(UnsignedLong.fromLongBits(right.getTime(columnIndex)));
                            break;
                        }
                        case DATE_TYPE: {
                            res = UnsignedLong.fromLongBits(left.getDate(columnIndex)).compareTo(UnsignedLong.fromLongBits(right.getDate(columnIndex)));
                            break;
                        }
                        case DATETIME_MILLI_TYPE: {
                            res = UnsignedLong.fromLongBits(left.getDatetime(columnIndex)).compareTo(UnsignedLong.fromLongBits(right.getDatetime(columnIndex)));
                            break;
                        }
                        case SYMBOL_TYPE: {
                            res = left.getSymbol(columnIndex).toString().compareTo((right.getSymbol(columnIndex)).toString());
                            break;
                        }
                        case OBJECT_TYPE:
                        case NULL_TYPE:
                        default:
                            throw new IllegalArgumentException();
                    }
                    if (res == 0){

                    }else {
                        return res;
                    }
                }
                return 0;
            }
        };
    }

    public static void setTinyIntVector(TinyIntVector valueVector, int index, byte value) {
        valueVector.set(index, value);
    }

    public static void setTinyIntVectorNull(TinyIntVector valueVector, int index) {
        valueVector.setNull(index);
    }

    public static void setUInt1Vector(UInt1Vector valueVector, int index, byte value) {
        valueVector.set(index, value);
    }

    public static void setUInt1VectorNull(UInt1Vector valueVector, int index) {
        valueVector.setNull(index);
    }

    public static void setSmallIntVector(SmallIntVector valueVector, int index, short value) {
        valueVector.set(index, value);
    }

    public static void setSmallIntVector(SmallIntVector valueVector, int index) {
        valueVector.setNull(index);
    }

    public static void setUInt2Vector(UInt2Vector valueVector, int index, short value) {
        valueVector.set(index, value);
    }

    public static void setUInt2VectorNull(UInt2Vector valueVector, int index) {
        valueVector.setNull(index);
    }

    public static void setUInt4Vector(UInt4Vector valueVector, int index, int value) {
        valueVector.set(index, value);
    }

    public static void setUInt4VectorNull(UInt4Vector valueVector, int index) {
        valueVector.setNull(index);
    }

    public static void setUInt8Vector(UInt8Vector valueVector, int index, int value) {
        valueVector.set(index, value);
    }

    public static void setUInt8VectorNull(UInt8Vector valueVector, int index) {
        valueVector.setNull(index);
    }

    public static void setIntVector(IntVector valueVector, int index, int value) {
        valueVector.set(index, value);
    }

    public static void setIntVectorNull(IntVector valueVector, int index) {
        valueVector.setNull(index);
    }

    public static void setBigIntVector(BigIntVector valueVector, int index, long value) {
        valueVector.set(index, value);
    }

    public static void setBigIntVectorNull(BigIntVector valueVector, int index) {
        valueVector.setNull(index);
    }

    public static void setDoubleVector(Float8Vector valueVector, int index, double value) {
        valueVector.set(index, value);
    }

    public static void setFloat8VectorNull(Float8Vector valueVector, int index) {
        valueVector.setNull(index);
    }

    public static void setFloat4Vector(Float4Vector valueVector, int index, float value) {
        valueVector.set(index, value);
    }

    public static void setFloat4VectorNull(Float4Vector valueVector, int index) {
        valueVector.setNull(index);
    }

    public static void setVarCharVector(VarCharVector valueVector, int index, String value) {
        valueVector.set(index, new Text(value));
    }

    public static void setVarCharVectorNull(VarCharVector valueVector, int index) {
        valueVector.setNull(index);
    }

    public static void setVarCharVector(VarCharVector valueVector, int index, byte[] value) {
        valueVector.set(index, value);
    }

    public static void setVarBinaryVector(VarBinaryVector valueVector, int index, byte[] value) {
        valueVector.set(index, value);
    }

    public static void setVarBinaryVector(VarBinaryVector valueVector, int index) {
        valueVector.setNull(index);
    }
}
