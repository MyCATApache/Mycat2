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
import org.apache.calcite.linq4j.Enumerator;

public class MycatJoinExecutor implements Executor {
    private final Executor[] executors;
    private Enumerable<Row> rows;
    private Enumerator<Row> enumerator;

    public MycatJoinExecutor(Executor[] executors, Enumerable<Row> rows) {
        this.executors = executors;
        this.rows = rows;
    }

    @Override
    public void open() {
        for (Executor executor : executors) {
            executor.open();
        }
        this.enumerator = rows.enumerator();
    }

    @Override
    public Row next() {
        if(enumerator.moveNext()){
            return enumerator.current();
        }
        return null;
    }

    @Override
    public void close() {
        for (Executor executor : executors) {
            executor.close();
        }
    }
}