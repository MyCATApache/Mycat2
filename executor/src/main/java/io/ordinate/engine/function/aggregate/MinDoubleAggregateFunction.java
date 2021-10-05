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

public class MinDoubleAggregateFunction implements DoubleAccumulator {
    private int columnIndex;
    private int stackIndex;

    public MinDoubleAggregateFunction(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    @Override
    public String name() {
        return "min(double)";
    }

    @Override
    public void computeFirst(MapValue reduceContext, Record record) {
        reduceContext.putDouble(stackIndex, Double.MAX_VALUE);
        computeNext(reduceContext, record);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        double value = record.getDouble(columnIndex);
        boolean isNull = record.isNull(columnIndex);
        if (!isNull) {
            double old = mapValue.getDouble(stackIndex);
            if (old > value) {
                mapValue.putDouble(stackIndex, value);
            }
        }
    }

    @Override
    public void allocContext(ArrayColumnTypes columnTypes) {
        stackIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.DOUBLE);
    }

    @Override
    public double getDouble(Record rec) {
        return rec.getDouble(stackIndex);
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
        return InnerType.DOUBLE_TYPE;
    }
}
