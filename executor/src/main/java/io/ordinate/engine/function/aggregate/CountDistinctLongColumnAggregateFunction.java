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

import com.carrotsearch.hppc.LongHashSet;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.record.Record;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;

import java.util.ArrayList;
import java.util.List;

public class CountDistinctLongColumnAggregateFunction implements LongAccumulator {
    private int inputColumn;
    final List<LongHashSet> longCursors = new ArrayList<>();
    private int stackIndex;
    private int valueIndex;

    public CountDistinctLongColumnAggregateFunction(int inputColumn) {
        this.inputColumn = inputColumn;
    }

    @Override
    public String name() {
        return "count(int64)";
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        LongHashSet longCursors;
        if (this.longCursors.size() <= valueIndex) {
            this.longCursors.add(longCursors = new LongHashSet());
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
        columnTypes.add(ColumnType.LONG);
        columnTypes.add(ColumnType.INT);
    }


    @Override
    public void computeNext(MapValue resultValue, Record record) {
        long value = record.getLong(inputColumn);
        if (record.isNull(inputColumn)) {
            return;
        }
        LongHashSet doubleCursors = this.longCursors.get(resultValue.getInt(stackIndex + 1));
        if (!doubleCursors.contains(value)) {
            doubleCursors.add(value);
            resultValue.addLong(stackIndex, 1);
        }
    }

    @Override
    public void init(int columnIndex) {
        this.inputColumn = columnIndex;
    }


    @Override
    public int getInputColumnIndex() {
        return inputColumn;
    }

    @Override
    public InnerType getType() {
        return InnerType.INT64_TYPE;
    }

    @Override
    public long getLong(Record rec) {
        LongHashSet doubleCursors = this.longCursors.get(rec.getInt(stackIndex + 1));
         return doubleCursors.size();
    }

    @Override
    public double getDouble(Record rec) {
        return getLong(rec);
    }

    @Override
    public boolean isNull(Record rec) {
        return false;
    }
}
