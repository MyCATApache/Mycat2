package io.mycat.compute.expression;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.mysqlapi.collector.RowBaseIterator;
import java.io.InputStream;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.LinkedList;

public class TableScan implements RowBaseIterator {

  final MycatRowMetaData metaData;
  final LinkedList<RowBaseIterator> dataNodeRowIter = new LinkedList<>();
  RowBaseIterator currentRowBaseIterator = null;

  public TableScan(MycatRowMetaData metaData) {
    this.metaData = metaData;
  }

  @Override
  public MycatRowMetaData metaData() {
    return metaData;
  }

  @Override
  public boolean next() {
    if (currentRowBaseIterator == null) {
      if (dataNodeRowIter.isEmpty()) {
        return false;
      } else {
        currentRowBaseIterator = dataNodeRowIter.removeFirst();
        return next();
      }
    } else {
      if (currentRowBaseIterator.next()) {
        return true;
      } else {
        currentRowBaseIterator.close();
        currentRowBaseIterator = null;
        return next();
      }
    }
  }

  @Override
  public void close() {
    Iterator<RowBaseIterator> iterator = dataNodeRowIter.iterator();
    while (iterator.hasNext()) {
      iterator.next().close();
      iterator.remove();
    }
  }

  @Override
  public boolean wasNull() {
    return currentRowBaseIterator.wasNull();
  }

  @Override
  public String getString(int columnIndex) {
    return currentRowBaseIterator.getString(columnIndex);
  }

  @Override
  public boolean getBoolean(int columnIndex) {
    return currentRowBaseIterator.getBoolean(columnIndex);
  }

  @Override
  public byte getByte(int columnIndex) {
    return currentRowBaseIterator.getByte(columnIndex);
  }

  @Override
  public short getShort(int columnIndex) {
    return currentRowBaseIterator.getShort(columnIndex);
  }

  @Override
  public int getInt(int columnIndex) {
    return currentRowBaseIterator.getInt(columnIndex);
  }

  @Override
  public long getLong(int columnIndex) {
    return currentRowBaseIterator.getLong(columnIndex);
  }

  @Override
  public float getFloat(int columnIndex) {
    return currentRowBaseIterator.getFloat(columnIndex);
  }

  @Override
  public double getDouble(int columnIndex) {
    return currentRowBaseIterator.getFloat(columnIndex);
  }

  @Override
  public byte[] getBytes(int columnIndex) {
    return currentRowBaseIterator.getBytes(columnIndex);
  }

  @Override
  public Date getDate(int columnIndex) {
    return currentRowBaseIterator.getDate(columnIndex);
  }

  @Override
  public Time getTime(int columnIndex) {
    return currentRowBaseIterator.getTime(columnIndex);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) {
    return currentRowBaseIterator.getTimestamp(columnIndex);
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) {
    return currentRowBaseIterator.getAsciiStream(columnIndex);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) {
    return currentRowBaseIterator.getBinaryStream(columnIndex);
  }

  @Override
  public Object getObject(int columnIndex) {
    return currentRowBaseIterator.getObject(columnIndex);
  }
}