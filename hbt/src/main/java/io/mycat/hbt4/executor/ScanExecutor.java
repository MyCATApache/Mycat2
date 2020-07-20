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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ScanExecutor implements Executor {

    private Iterator<Row> iter;

    @Override
    public void open() {
        List<Object[]> objects = Arrays.asList(new Object[]{1L},new Object[]{2L});
        this.iter = objects.stream().map(i->new Row(i)).iterator();
    }

    @Override
    public Row next() {
        boolean b = iter.hasNext();
        if (b){
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
}