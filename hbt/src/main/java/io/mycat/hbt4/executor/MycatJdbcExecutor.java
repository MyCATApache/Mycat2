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

import io.mycat.calcite.table.MycatSQLTableScan;
import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;

import java.util.Iterator;

public class MycatJdbcExecutor implements Executor {


    private MycatSQLTableScan tableScan;
    private Iterator<Object[]> iterator;

    protected MycatJdbcExecutor(MycatSQLTableScan tableScan) {
        this.tableScan = tableScan;
    }

    public MycatJdbcExecutor create(MycatSQLTableScan tableScan) {
        return new MycatJdbcExecutor(tableScan);
    }

    @Override
    public void open() {
        this.iterator = tableScan.scan(null).iterator();
    }

    @Override
    public Row next() {
        if (iterator.hasNext()) {
            return Row.of(iterator.next());
        } else {
            return null;
        }
    }


    @Override
    public void close() {

    }

    @Override
    public boolean isRewindSupported() {
        return false;
    }
}