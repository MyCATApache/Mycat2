package io.mycat.api.collector;

import io.mycat.beans.mycat.MycatRowMetaData;
import lombok.SneakyThrows;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * chenjunwen
 */
public class BlockQueueRowBaseIterator implements RowBaseIterator {
    final BlockingQueue<Object[]> queue;

    final long timeout;
    final TimeUnit unit;
    volatile Runnable closeCallback;

    public BlockQueueRowBaseIterator(BlockingQueue<Object[]> queue, MycatRowMetaData mycatRowMetaData, long timeout, TimeUnit unit, Runnable closeCallback) {
        this.queue = queue;
        this.mycatRowMetaData = mycatRowMetaData;
        this.timeout = timeout;
        this.unit = unit;
        this.closeCallback = closeCallback;
    }

    Object[] row;
    int index;
    volatile MycatRowMetaData mycatRowMetaData;

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
        }else {
            return false;
        }
    }

    @Override
    public synchronized void close() {
        if (closeCallback!=null) {
            closeCallback.run();
            closeCallback = null;
        }
    }

    @Override
    public boolean wasNull() {
        return row[index-1] == null;
    }

    @Override
    public String getString(int columnIndex) {
        return row[index-1].toString();
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
        return row[columnIndex-1];
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) {
        throw new UnsupportedOperationException();
    }

}