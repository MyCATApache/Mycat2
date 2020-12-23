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
import org.apache.calcite.linq4j.Linq4j;

import java.util.Arrays;
import java.util.Iterator;

public class ScanExecutor implements Executor {

    private Iterator<Row> iter;

    public ScanExecutor(Iterator<Row> iter) {
        this.iter = iter;
    }

    @Override
    public void open() {

    }

    public static ScanExecutor createDemo() {
        return new ScanExecutor(Arrays.asList(
                new Object[]{1L, 1L, 1L, 1L, 1L, 1L, 1L},
                new Object[]{1L, 1L, 1L, 1L, 1L, 1L, 1L}
        ).stream().map(r -> Row.of(r)).iterator());
    }

    public static ScanExecutor create(Iterator<Row> iter) {
        return new ScanExecutor(iter);
    }

    @Override
    public Row next() {
        boolean b = iter.hasNext();
        if (b) {
            return iter.next();
        }
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isRewindSupported() {
        return false;
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        ExplainWriter explainWriter = writer.name(this.getClass().getName())
                .into();
        writer.item("row", Linq4j.asEnumerable(()->iter).toList());
        return explainWriter.ret();
    }
}