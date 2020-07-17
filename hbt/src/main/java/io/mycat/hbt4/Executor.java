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
package io.mycat.hbt4;

import io.mycat.mpp.Row;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public interface Executor extends Iterable<Row>{
    public void open();

    public Row next();

    public void close();

    @NotNull
    @Override
    default Iterator<Row> iterator() {
        return new Iterator<Row>() {
            Row row;

            @Override
            public boolean hasNext() {
                row = Executor.this.next();
                return row != null;
            }

            @Override
            public Row next() {
                return row;
            }
        };
    }

    public  boolean isRewindSupported() ;


    @NotNull
    default Iterator<Object[]> outputObjectIterator() {
        return new Iterator<Object[]>() {
            Object[] row;

            @Override
            public boolean hasNext() {
                Row next = Executor.this.next();
                if (next != null){
                    row = next.values;
                    return true;
                }else {
                    return false;
                }
            }

            @Override
            public Object[] next() {
                return row;
            }
        };
    }
}