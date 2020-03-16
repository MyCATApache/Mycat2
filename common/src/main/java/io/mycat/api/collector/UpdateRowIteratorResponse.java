package io.mycat.api.collector;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.UpdateRowMetaData;
import io.mycat.beans.resultset.MycatUpdateResponse;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;


public class UpdateRowIteratorResponse implements RowBaseIterator, MycatUpdateResponse {
    protected boolean next = false;
    protected long updateCount;
    protected long lastInsertId;
    public final int serverStatus;

    public UpdateRowIteratorResponse(long updateCount, long lastInsertId, int serverStatus) {
        this.updateCount = updateCount;
        this.lastInsertId = lastInsertId;
        this.serverStatus = serverStatus;
        this.next = false;
    }

    @Override
    public MycatRowMetaData getMetaData() {
        return UpdateRowMetaData.INSTANCE;
    }

    @Override
    public boolean next() {
        if (!next) {
            next = true;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public boolean wasNull() {
        return false;
    }

    @Override
    public String getString(int columnIndex) {
        throw new UnsupportedOperationException();
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
        switch (columnIndex) {
            case 1:
                return updateCount;
            case 2:
                return lastInsertId;
            default:
        }
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
    public Date getDate(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) {
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
        throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getUpdateCount() {
        return updateCount;
    }

    @Override
    public long getLastInsertId() {
        return lastInsertId;
    }

    @Override
    public int serverStatus() {
        return serverStatus;
    }
}