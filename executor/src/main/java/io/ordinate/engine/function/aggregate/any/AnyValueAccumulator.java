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

package io.ordinate.engine.function.aggregate.any;

import io.ordinate.engine.function.BinarySequence;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.record.Record;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AnyValueAccumulator implements AnyAccumulator {
    final InnerType type;
    int inputColumnIndex;
    int stackIndex;
    int valueIndex;
    List<Object> object = new ArrayList<>();

    public AnyValueAccumulator(InnerType type, int inputColumnIndex) {
        this.type = type;
        this.inputColumnIndex = inputColumnIndex;
    }

    @Override
    public String name() {
        return "anyValue";
    }

    @Override
    public void computeFirst(MapValue reduceContext, Record record) {
        if (object.size()<=valueIndex){
            object.add(null);
        }
        reduceContext.putLong(stackIndex,valueIndex);
        switch (type) {
            case BOOLEAN_TYPE: {
                int value = record.getInt(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull){
                    object.set(valueIndex,value);
                }
                break;
            }
            case INT8_TYPE: {
                byte value = record.getByte(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull) {
                    object.set(valueIndex, value);
                }
                break;
            }
            case INT16_TYPE:
            {
                short value = record.getShort(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull) {
                    object.set(valueIndex, value);
                }
                break;
            }
            case CHAR_TYPE:
            {
                char value = record.getChar(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull) {
                    object.set(valueIndex, value);
                }
                break;
            }
            case INT32_TYPE:
            {
                int value = record.getInt(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull) {
                    object.set(valueIndex, value);
                }
                break;
            }
            case INT64_TYPE:
            {
                long value = record.getLong(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull) {
                    object.set(valueIndex, value);
                }
                break;
            }
            case FLOAT_TYPE:
            {
                float value = record.getFloat(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull) {
                    object.set(valueIndex, value);
                }
                break;
            }
            case DOUBLE_TYPE:
            {
                double value = record.getDouble(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull) {
                    object.set(valueIndex, value);
                }
                break;
            }
            case STRING_TYPE:
            {
                CharSequence value = record.getString(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull) {
                    object.set(valueIndex, value);
                }
                break;
            }
            case BINARY_TYPE:
            {
                BinarySequence value = record.getBinary(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull) {
                    object.set(valueIndex, value);
                }
                break;
            }
            case UINT8_TYPE:
            {
                byte value = record.getByte(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull) {
                    object.set(valueIndex, value);
                }
                break;
            }
            case UINT16_TYPE:
            {
                short value = record.getShort(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull) {
                    object.set(valueIndex, value);
                }
                break;
            }
            case UINT32_TYPE:
            {
                int value = record.getInt(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull) {
                    object.set(valueIndex, value);
                }
                break;
            }
            case UINT64_TYPE:
            {
                long value = record.getLong(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull) {
                    object.set(valueIndex, value);
                }
                break;
            }
            case TIME_MILLI_TYPE:
            {
                long value = record.getTime(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull) {
                    object.set(valueIndex, value);
                }
                break;
            }
            case DATE_TYPE:
            {
                long value = record.getDate(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull) {
                    object.set(valueIndex, value);
                }
                break;
            }
            case DATETIME_MILLI_TYPE:
            {
                long value = record.getDatetime(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull) {
                    object.set(valueIndex, value);
                }
                break;
            }
            case SYMBOL_TYPE:
            {
                CharSequence value = record.getSymbol(inputColumnIndex);
                boolean isNull = record.isNull(inputColumnIndex);
                if (!isNull) {
                    object.set(valueIndex, value);
                }
                break;
            }
            case OBJECT_TYPE:
            {
              throw new UnsupportedOperationException();
            }
            case NULL_TYPE:
            {
                object.set(valueIndex, null);
                break;
            }
        }

        valueIndex++;
    }

    @Override
    public void computeNext(MapValue resultValue, Record record) {

    }

    @Override
    public void allocContext(ArrayColumnTypes columnTypes) {
        stackIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.BOOLEAN);
    }

    @Override
    public int getInputColumnIndex() {
        return this.inputColumnIndex;
    }

    @Override
    public InnerType getOutputType() {
        return type;
    }

    @Override
    public InnerType getInputType() {
        return type;
    }

    @Override
    public InnerType getType() {
        return type;
    }

    @Override
    public void setInputColumnIndex(int index) {
        this.inputColumnIndex = index;
    }


    @Override
    public BinarySequence getBinary(Record rec) {
        int valueIndex = rec.getInt(stackIndex);
        Object o = object.get(valueIndex);
        if (o == null)return null;
        return (BinarySequence)o;
    }

    @Override
    public byte getByte(Record rec) {
        int valueIndex = rec.getInt(stackIndex);
        Object o = object.get(valueIndex);
        if (o == null)return 0;
        return (Byte) o;
    }

    @Override
    public char getChar(Record rec) {
        int valueIndex = rec.getInt(stackIndex);
        Object o = object.get(valueIndex);
        if (o == null)return 0;
        return (Character) o;
    }

    @Override
    public long getDate(Record rec) {
        int valueIndex = rec.getInt(stackIndex);
        Object o = object.get(valueIndex);
        if (o == null)return 0;
        return (Long) o;
    }

    @Override
    public double getDouble(Record rec) {
        int valueIndex = rec.getInt(stackIndex);
        Object o = object.get(valueIndex);
        if (o == null)return 0;
        return (Double) o;
    }

    @Override
    public float getFloat(Record rec) {
        int valueIndex = rec.getInt(stackIndex);
        Object o = object.get(valueIndex);
        if (o == null)return 0;
        return (Float) o;
    }

    @Override
    public int getInt(Record rec) {
        int valueIndex = rec.getInt(stackIndex);
        Object o = object.get(valueIndex);
        if (o == null)return 0;
        return (Integer) o;
    }

    @Override
    public long getLong(Record rec) {
        int valueIndex = rec.getInt(stackIndex);
        Object o = object.get(valueIndex);
        if (o == null)return 0;
        return (Long) o;
    }

    @Override
    public short getShort(Record rec) {
        int valueIndex = rec.getInt(stackIndex);
        Object o = object.get(valueIndex);
        if (o == null)return 0;
        return (Short) o;
    }

    @Override
    public CharSequence getString(Record rec) {
        int valueIndex = rec.getInt(stackIndex);
        Object o = object.get(valueIndex);
        if (o == null)return null;
        return (CharSequence) o;
    }

    @Override
    public long getDatetime(Record rec) {
        int valueIndex = rec.getInt(stackIndex);
        Object o = object.get(valueIndex);
        if (o == null)return 0;
        return (Long) o;
    }

    @Override
    public long getTime(Record rec) {
        int valueIndex = rec.getInt(stackIndex);
        Object o = object.get(valueIndex);
        if (o == null)return 0;
        return (Long) o;
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        int valueIndex = rec.getInt(stackIndex);
        Object o = object.get(valueIndex);
        if (o == null)return null;
        return (CharSequence) o;
    }

    @Override
    public boolean isNull(Record rec) {
        int valueIndex = rec.getInt(stackIndex);
        Object o = object.get(valueIndex);
        return o == null;
    }
}
