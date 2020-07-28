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

import java.util.function.Function;

public class MycatProjectExecutor implements Executor {
    private Function<Row, Row> mycatScalar;
    private Executor executor;

    protected MycatProjectExecutor(Function<Row, Row> mycatScalar, Executor executor) {
        this.mycatScalar = mycatScalar;
        this.executor = executor;
    }

    public MycatProjectExecutor create(Function<Row, Row> mycatScalar, Executor executor) {
        return new MycatProjectExecutor(
                mycatScalar,
                executor
        );
    }

    @Override
    public void open() {
        executor.open();
    }

    @Override
    public Row next() {
        Row next = executor.next();
        if (next == null) {
            return null;
        }
        return mycatScalar.apply(next);
    }

    @Override
    public void close() {
        executor.close();
    }

    @Override
    public boolean isRewindSupported() {
        return executor.isRewindSupported();
    }
}