package io.mycat.api.collector;

import io.mycat.beans.mycat.MycatRowMetaData;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Objects;

public abstract class AbstractObjectRowIterator implements RowBaseIterator {
    protected final MycatRowMetaData mycatRowMetaData;
    protected final Iterator<Object[]> iterator;
    private Object[] currentRow;
    private boolean wasNull;

    public AbstractObjectRowIterator(MycatRowMetaData mycatRowMetaData, Iterator<Object[]> iterator) {
        this.mycatRowMetaData = mycatRowMetaData;
        this.iterator = iterator;
    }

    @Override
    public MycatRowMetaData metaData() {
        return mycatRowMetaData;
    }

    @Override
    public boolean next() {
        if (this.iterator.hasNext()) {
            this.currentRow = this.iterator.next();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return Objects.toString(o);
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return false;
        return (Boolean) o;
    }

    @Override
    public byte getByte(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return 0;
        return (Byte) o;
    }

    @Override
    public short getShort(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return 0;
        return (Short) o;
    }

    @Override
    public int getInt(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return 0;
        return (Byte) o;
    }

    @Override
    public long getLong(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return 0;
        return (Byte) o;
    }

    @Override
    public float getFloat(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return 0;
        return (Byte) o;
    }

    @Override
    public double getDouble(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return 0;
        return (Double) o;
    }

    @Override
    public byte[] getBytes(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return (byte[]) o;
    }

    @Override
    public Date getDate(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return (Date) o;
    }

    @Override
    public Time getTime(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return (Time) o;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return (Timestamp) o;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return (InputStream) o;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return (InputStream) o;
    }

    @Override
    public Object getObject(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        return o;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) {
        Object o = getObject(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return (BigDecimal) o;
    }

    private Object getObject(Object o1) {
        Object o = o1;
        wasNull = null == o;
        return o;
    }
}