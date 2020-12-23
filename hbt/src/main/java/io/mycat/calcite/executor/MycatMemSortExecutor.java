/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite.executor;

import io.mycat.calcite.Executor;
import io.mycat.calcite.ExplainWriter;
import io.mycat.mpp.Row;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class MycatMemSortExecutor implements Executor {
    private final Executor input;
    private final Comparator<Row> comparator;
    private Iterator<Row> iterator;
    private List<Row> output = null;

    protected MycatMemSortExecutor(Comparator<Row> comparator, Executor input) {
        this.comparator = comparator;
        this.input = input;
    }
   public static  MycatMemSortExecutor create(Comparator<Row> comparator, Executor input) {
       return new MycatMemSortExecutor(comparator, input);
    }
    @Override
    public void open() {
        if (output == null) {
            input.open();
            output = StreamSupport.stream( input.spliterator(),false).parallel().sorted(comparator).collect(Collectors.toList());
            input.close();
            this.iterator = output.iterator();
        }
    }

    @Override
    public Row next() {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public void close() {
        input.close();
        output = null;
    }

    @Override
    public boolean isRewindSupported() {
        return true;
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        ExplainWriter explainWriter = writer.name(this.getClass().getName())
                .into();
        input.explain(writer);
        return explainWriter.ret();
    }
}