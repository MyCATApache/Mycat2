package io.mycat.api.collector;

import io.mycat.beans.mycat.MycatRowMetaData;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
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
        if(current == null){
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
    public java.util.Date getDate(int columnIndex) {
        return current.getDate(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) {
        return current.getTime(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) {
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