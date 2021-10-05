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
import io.ordinate.engine.function.aggregate.AccumulatorFunction;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.map.MapValue;
import org.jetbrains.annotations.NotNull;

public class RecordUtil {
    public static Record wrapAsAggRecord(MapValue value) {
        return new Record() {
            @Override
            public void setPosition(int value) {

            }

            @Override
            public int getInt(int columnIndex) {
                return value.getInt(columnIndex);
            }

            @Override
            public long getLong(int columnIndex) {
                return value.getLong(columnIndex);
            }

            @Override
            public byte getByte(int columnIndex) {
                return 0;
            }

            @Override
            public short getShort(int columnIndex) {
                return 0;
            }

            @Override
            public BinarySequence getBinary(int columnIndex) {
                return null;
            }

            @Override
            public char getChar(int columnIndex) {
                return 0;
            }

            @Override
            public long getDate(int columnIndex) {
                return 0;
            }

            @Override
            public long getDatetime(int columnIndex) {
                return 0;
            }

            @Override
            public CharSequence getString(int columnIndex) {
                return null;
            }

            @Override
            public CharSequence getSymbol(int columnIndex) {
                return null;
            }

            @Override
            public float getFloat(int columnIndex) {
                return 0;
            }

            @Override
            public double getDouble(int columnIndex) {
                return value.getDouble(columnIndex);
            }

            @Override
            public long getTime(int columnIndex) {
                return 0;
            }

            @Override
            public boolean isNull(int columnIndex) {
                return false;
            }

            @Override
            public short getUInt16(int columnIndex) {
                return 0;
            }

            @Override
            public long getUInt64(int columnIndex) {
                return 0;
            }

            @Override
            public Object getObject(int columnIndex) {
                return null;
            }

            @Override
            public int getUInt32(int i) {
                return 0;
            }
        };
    }

    public static int getContextSize(AccumulatorFunction[] aggregateExprs) {
        ArrayColumnTypes arrayColumnTypes = getArrayColumnTypes(aggregateExprs);
        return arrayColumnTypes.getColumnCount();
    }

    @NotNull
    public static ArrayColumnTypes getArrayColumnTypes(AccumulatorFunction[] aggregateExprs) {
        ArrayColumnTypes arrayColumnTypes = new ArrayColumnTypes();
        for (AccumulatorFunction aggregateExpr : aggregateExprs) {
            aggregateExpr.allocContext(arrayColumnTypes);
        }
        return arrayColumnTypes;
    }

    public static Record wrapAsAggRecord(io.questdb.cairo.sql.Record record) {
        return new Record() {
            @Override
            public void setPosition(int value) {

            }

            @Override
            public int getInt(int columnIndex) {
                return record.getInt(columnIndex);
            }

            @Override
            public long getLong(int columnIndex) {
                return record.getLong(columnIndex);
            }

            @Override
            public byte getByte(int columnIndex) {
                return 0;
            }

            @Override
            public short getShort(int columnIndex) {
                return 0;
            }

            @Override
            public BinarySequence getBinary(int columnIndex) {
                return null;
            }

            @Override
            public char getChar(int columnIndex) {
                return 0;
            }

            @Override
            public long getDate(int columnIndex) {
                return 0;
            }

            @Override
            public long getDatetime(int columnIndex) {
                return 0;
            }

            @Override
            public CharSequence getString(int columnIndex) {
                return null;
            }


            @Override
            public float getFloat(int columnIndex) {
                return 0;
            }

            @Override
            public double getDouble(int columnIndex) {
                return record.getDouble(columnIndex);
            }

            @Override
            public long getTime(int columnIndex) {
                return 0;
            }

            @Override
            public boolean isNull(int columnIndex) {
                return false;
            }

            @Override
            public short getUInt16(int columnIndex) {
                return 0;
            }

            @Override
            public long getUInt64(int columnIndex) {
                return 0;
            }

            @Override
            public String getSymbol(int columnIndex) {
                return null;
            }

            @Override
            public Object getObject(int columnIndex) {
                return null;
            }

            @Override
            public int getUInt32(int i) {
                return 0;
            }
        };
    }

}
