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

import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.record.Record;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;

public class MaxLongAggregateFunction implements LongAccumulator {
    private int columnIndex;
    private int stackIndex;

    public MaxLongAggregateFunction(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    @Override
    public String name() {
        return "max(int64)";
    }

    @Override
    public void computeFirst(MapValue reduceContext, Record record) {
        reduceContext.putLong(stackIndex, Long.MIN_VALUE);
        computeNext(reduceContext, record);
    }

    @Override
    public void computeNext(MapValue resultValue, Record record) {
        long value = record.getLong(columnIndex);
        boolean isNull = record.isNull(columnIndex);
        if (!isNull) {
            double old = resultValue.getLong(columnIndex);
            if (old < value) {
                resultValue.putDouble(stackIndex, value);
            }
        }
    }
    @Override
    public long getLong(Record rec) {
        return rec.getLong(stackIndex);
    }

    @Override
    public void allocContext(ArrayColumnTypes columnTypes) {
        stackIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.DOUBLE);
    }

    @Override
    public void init(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    @Override
    public int getInputColumnIndex() {
        return columnIndex;
    }

    @Override
    public InnerType getType() {
        return InnerType.INT64_TYPE;
    }
}
