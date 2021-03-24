/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.calcite.resultset;

import io.mycat.api.collector.RowBaseIterator;
import org.apache.calcite.linq4j.Enumerator;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyCatResultSetEnumerator implements Enumerator<Object[]> {
    private final static Logger LOGGER = LoggerFactory.getLogger(MyCatResultSetEnumerator.class);

    final RowBaseIterator rowBaseIterator;
    final int columnCount;
    boolean result = true;

    public MyCatResultSetEnumerator(RowBaseIterator rowBaseIterator) {
        this.rowBaseIterator = rowBaseIterator;
        this.columnCount = rowBaseIterator.getMetaData().getColumnCount();
    }


    @Override
    public Object[] current() {
        Object[] res = new Object[columnCount];

        for (int i = 0, j = 1; i < columnCount; i++, j++) {
            res[i] = rowBaseIterator.getObject(j);
        }
        return (Object[]) res;
    }

    @Override
    public boolean moveNext() {
        if (result) {
            result = rowBaseIterator.next();
            return result;
        }

        return false;
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        if (rowBaseIterator != null) {
            rowBaseIterator.close();
        }
    }
}