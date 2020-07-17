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

public class MycatMultiViewExecutor implements Executor {
    int index = 0;
    final Executor[] executors;

    public MycatMultiViewExecutor(Executor[] executors) {
        this.executors = executors;
    }

    @Override
    public void open() {
        for (Executor executor : executors) {
            executor.open();
        }
    }

    @Override
    public Row next() {
        Executor executor = executors[index];
        Row row = executor.next();
        if (row == null) {
            executor.close();
            index++;
            if (index >= executors.length) {
                return null;
            } else {
                return next();
            }
        }
        return row;
    }

    @Override
    public void close() {
        for (Executor executor : executors) {
            executor.close();
        }
    }

    @Override
    public boolean isRewindSupported() {
        return false;
    }
}