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

public class AvgAggregateFunction implements DoubleAccumulator {
    private int columnIndex;
    private int stackIndex;

    public AvgAggregateFunction(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    @Override
    public String name() {
        return "avg(double)";
    }

    @Override
    public void computeFirst(MapValue reduceContext, Record record) {
        reduceContext.putLong(stackIndex,0);
        reduceContext.putDouble(stackIndex+1,0);
        computeNext(reduceContext,record);
    }

    @Override
    public void computeNext(MapValue resultValue, Record record) {
        double value = record.getDouble(columnIndex);
        boolean isNull = record.isNull(columnIndex);
        if (!isNull){
            resultValue.addLong(stackIndex,1);
            resultValue.addDouble(stackIndex+1,value);
        }
    }

    @Override
    public void allocContext(ArrayColumnTypes columnTypes) {
        this.stackIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG);//count
        columnTypes.add(ColumnType.DOUBLE);//sum
    }

    @Override
    public double getDouble(Record rec) {
        long count = rec.getLong(stackIndex);
        if (count == 0)return 0;
        double sum = rec.getDouble(stackIndex+1);
        return sum/count;
    }

    @Override
    public boolean isNull(Record rec) {
        long count = rec.getLong(stackIndex);
        if (count == 0)return true;
        return false;
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
