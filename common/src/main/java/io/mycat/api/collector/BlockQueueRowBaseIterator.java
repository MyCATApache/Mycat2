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

import java.io.InputStream;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * chenjunwen
 */
public class BlockQueueRowBaseIterator implements RowBaseIterator {
    final BlockingQueue<Object[]> queue;

    final long timeout;
    final TimeUnit unit;
    volatile Runnable closeCallback;
    Object[] row;
    int index;
    volatile MycatRowMetaData mycatRowMetaData;
    public BlockQueueRowBaseIterator(BlockingQueue<Object[]> queue, MycatRowMetaData mycatRowMetaData, long timeout, TimeUnit unit, Runnable closeCallback) {
        this.queue = queue;
        this.mycatRowMetaData = mycatRowMetaData;
        this.timeout = timeout;
        this.unit = unit;
        this.closeCallback = closeCallback;
    }

    @Override
    @SneakyThrows
    public MycatRowMetaData getMetaData() {
        return mycatRowMetaData;
    }

    @Override
    @SneakyThrows
    public boolean next() {
        if (row == null || row.length != 0) {
            row = queue.poll(timeout, unit);
            return row.length != 0;
        } else {
            return false;
        }
    }

    @Override
    public synchronized void close() {
        if (closeCallback != null) {
            closeCallback.run();
            closeCallback = null;
        }
    }

    @Override
    public boolean wasNull() {
        return row[index - 1] == null;
    }

    @Override
    public String getString(int columnIndex) {
        return row[index - 1].toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalDate getDate(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Duration getTime(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalDateTime getTimestamp(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(int columnIndex) {
        return row[columnIndex - 1];
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) {
        throw new UnsupportedOperationException();
    }

}