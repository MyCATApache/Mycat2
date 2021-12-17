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

package io.ordinate.engine.function.aggregate.count;

import io.ordinate.engine.function.aggregate.LongAccumulator;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.record.Record;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;

public class CountColumnAggregateFunction implements LongAccumulator {
    private int inputColumn;
    private InnerType innerType;
    protected int valueIndex;

    public CountColumnAggregateFunction(int inputColumn,InnerType innerType) {
        this.inputColumn = inputColumn;
        this.innerType = innerType;
    }

    @Override
    public String name() {
        return "countColumn()";
    }

    @Override
    public void computeFirst(MapValue reduceContext, Record record) {
        reduceContext.putLong(valueIndex, 0);
        computeNext(reduceContext,record);
    }

    @Override
    public void computeNext(MapValue resultValue, Record record) {
        if (record.isNull(inputColumn)) {
            return;
        }
        resultValue.addLong(valueIndex, 1);
    }

    @Override
    public void allocContext(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG);
    }

    @Override
    public long getLong(Record rec) {
        return rec.getLong(valueIndex);
    }

    @Override
    public int getInputColumnIndex() {
        return inputColumn;
    }

    @Override
    public InnerType getOutputType() {
        return getType();
    }

    @Override
    public InnerType getInputType() {
        return innerType;
    }

    @Override
    public InnerType getType() {
        return InnerType.INT64_TYPE;
    }

    @Override
    public void setInputColumnIndex(int index) {
        this.inputColumn = index;
    }
}
