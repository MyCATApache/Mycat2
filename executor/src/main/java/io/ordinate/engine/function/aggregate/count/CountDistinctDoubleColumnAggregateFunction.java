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


import io.ordinate.engine.function.aggregate.DoubleAccumulator;
import io.ordinate.engine.record.Record;
import io.ordinate.engine.schema.InnerType;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class CountDistinctDoubleColumnAggregateFunction implements DoubleAccumulator {
    private int inputColumn;
    final List<HashSet<Double>> longCursors = new ArrayList<>();
    private int stackIndex;
    private int valueIndex;

    public CountDistinctDoubleColumnAggregateFunction(int inputColumn) {
        this.inputColumn = inputColumn;
    }

    @Override
    public String name() {
        return "count(double)";
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        HashSet<Double> longCursors;
        if (this.longCursors.size() <= valueIndex) {
            this.longCursors.add(longCursors = new HashSet<Double>());
        } else {
            longCursors = this.longCursors.get(valueIndex);
        }
        longCursors.clear();


        mapValue.putInt(stackIndex + 1, valueIndex);
        valueIndex++;


        computeNext(mapValue,record);
    }

    @Override
    public void allocContext(ArrayColumnTypes columnTypes) {
        this.stackIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.INT);
    }


    @Override
    public void computeNext(MapValue resultValue, Record record) {
        double value = record.getDouble(inputColumn);
        if (record.isNull(inputColumn)) {
            return;
        }
        HashSet<Double> doubleCursors = this.longCursors.get(resultValue.getInt(stackIndex + 1));
        if (!doubleCursors.contains(value)) {
            doubleCursors.add(value);
            resultValue.addLong(stackIndex, 1);
        }
    }


    @Override
    public int getInputColumnIndex() {
        return inputColumn;
    }

    @Override
    public InnerType getOutputType() {
        return InnerType.DOUBLE_TYPE;
    }

    @Override
    public InnerType getInputType() {
        return InnerType.DOUBLE_TYPE;
    }

    @Override
    public InnerType getType() {
        return InnerType.DOUBLE_TYPE;
    }

    @Override
    public boolean isNull(Record rec) {
        return false;
    }

    @Override
    public void setInputColumnIndex(int index) {
        this.inputColumn = index;
    }
}
