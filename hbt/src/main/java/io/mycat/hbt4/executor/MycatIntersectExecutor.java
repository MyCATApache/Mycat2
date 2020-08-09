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
package io.mycat.hbt4.executor;

import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;

import java.util.Iterator;

public class MycatIntersectExecutor implements Executor {
    private Executor[] executors;
    private boolean all;
    private Iterator<Row> iterator;
    private Enumerable<Row> enumerables;

    protected MycatIntersectExecutor(Executor[] executors, boolean all) {
        this.executors = executors;
        this.all = all;
    }

    public static MycatIntersectExecutor create(Executor[] executors, boolean all) {
        return new MycatIntersectExecutor(executors,all);
    }

    @Override
    public void open() {
        if (enumerables == null) {
            for (Executor executor : executors) {
                executor.open();
            }
            Enumerable<Row> acc = Linq4j.asEnumerable(executors[0]);
            for (Executor i : executors) {
                if (i == executors[0]) continue;
                acc = acc.intersect(Linq4j.asEnumerable(i), all);
            }
            this.enumerables = acc;
            for (Executor executor : executors) {
                executor.close();
            }
        }
        this.iterator = this.enumerables.iterator();
    }

    @Override
    public Row next() {
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        for (Executor executor : executors) {
            executor.close();
        }
    }

    @Override
    public boolean isRewindSupported() {
        return true;
    }
};