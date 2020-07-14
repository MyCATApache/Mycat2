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

import java.util.function.Predicate;

public class MycatFilterExecutor implements Executor {
    private final Predicate<Row> predicate;
    private final Executor input;

    public MycatFilterExecutor(Predicate<Row> predicate, Executor input) {
        this.predicate = predicate;
        this.input = input;
    }

    @Override
    public void open() {
        input.open();
    }

    @Override
    public Row next() {
        Row row;
        do {
            row = input.next();
            if (row == null) {
                input.close();
                return null;
            }
        } while (predicate.test(row) != Boolean.TRUE);
        return row;
    }

    @Override
    public void close() {
        input.close();
    }
}