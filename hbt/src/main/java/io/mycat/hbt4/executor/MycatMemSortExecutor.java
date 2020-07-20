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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;


public class MycatMemSortExecutor implements Executor {
    private final Executor input;
    private final Comparator<Row> comparator;
    private Iterator<Row> iterator;
    private ArrayList<Row> output = null;

    public MycatMemSortExecutor(Comparator<Row> comparator, Executor input) {
        this.comparator = comparator;
        this.input = input;
    }

    @Override
    public void open() {
        if (output == null) {
            input.open();
            Iterator<Row> iterator = input.iterator();
            output = new ArrayList<>();
            while (iterator.hasNext()) {
                output.add(iterator.next());
            }
            input.close();
            output.sort(comparator);
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
    }

    @Override
    public boolean isRewindSupported() {
        return true;
    }
}