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
package io.mycat.api.collector;

import io.mycat.beans.mycat.MycatRowMetaData;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.concurrent.Future;

/**
 * 组合迭代器
 */
public class ComposeFutureRowBaseIterator implements RowBaseIterator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ComposeFutureRowBaseIterator.class);

    final LinkedList<Future<RowBaseIterator>> seq;
    private MycatRowMetaData metaData;
    private RowBaseIterator current;

    public ComposeFutureRowBaseIterator(MycatRowMetaData metaData, LinkedList<Future<RowBaseIterator>> seq) {
        this.metaData = metaData;
        this.seq = seq;
    }

    @Override
    public MycatRowMetaData getMetaData() {
        return metaData;
    }

    @Override
    @SneakyThrows
    public boolean next() {
        if (current == null && !seq.isEmpty()) {
            current = seq.removeFirst().get();
        }
        if (current == null) {
            return false;
        }
        boolean next = current.next();
        if (next) {
            return true;
        }
        try {
            current.close();
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        if (seq.isEmpty()) {
            return false;
        }
        current = seq.removeFirst().get();
        return next();
    }

    @Override
    public void close() {
        try {
            if (current != null) {
                current.close();
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        while (!seq.isEmpty()) {

            try {
                RowBaseIterator rowBaseIterator = seq.removeFirst().get();
                if (rowBaseIterator != null) {
                    rowBaseIterator.close();
                }
            } catch (Exception e) {
                LOGGER.error("", e);
            }
        }
    }

    @Override
    public boolean wasNull() {
        return current.wasNull();
    }

    @Override
    public String getString(int columnIndex) {
        return current.getString(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        return current.getBoolean(columnIndex);
    }

    @Override
    public byte getByte(int columnIndex) {
        return current.getByte(columnIndex);
    }

    @Override
    public short getShort(int columnIndex) {
        return current.getShort(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) {
        return current.getInt(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) {
        return current.getLong(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) {
        return current.getFloat(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) {
        return current.getDouble(columnIndex);
    }

    @Override
    public byte[] getBytes(int columnIndex) {
        return current.getBytes(columnIndex);
    }

    @Override
    public LocalDate getDate(int columnIndex) {
        return current.getDate(columnIndex);
    }

    @Override
    public Duration getTime(int columnIndex) {
        return current.getTime(columnIndex);
    }

    @Override
    public LocalDateTime getTimestamp(int columnIndex) {
        return current.getTimestamp(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) {
        return current.getAsciiStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) {
        return current.getBinaryStream(columnIndex);
    }

    @Override
    public Object getObject(int columnIndex) {
        return current.getObject(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) {
        return current.getBigDecimal(columnIndex);
    }
}