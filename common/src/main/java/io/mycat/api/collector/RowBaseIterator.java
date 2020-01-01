package io.mycat.api.collector;

import io.mycat.beans.mycat.MycatRowMetaData;

import java.io.Closeable;
import java.math.BigDecimal;

public interface RowBaseIterator extends Closeable {

  MycatRowMetaData metaData();

  boolean next();

  void close();

  boolean wasNull();

  String getString(int columnIndex);

  boolean getBoolean(int columnIndex);

  byte getByte(int columnIndex);

  short getShort(int columnIndex);

  int getInt(int columnIndex);

  long getLong(int columnIndex);

  float getFloat(int columnIndex);

  double getDouble(int columnIndex);

  byte[] getBytes(int columnIndex);

  java.sql.Date getDate(int columnIndex);

  java.sql.Time getTime(int columnIndex);

  java.sql.Timestamp getTimestamp(int columnIndex);

  java.io.InputStream getAsciiStream(int columnIndex);

  java.io.InputStream getBinaryStream(int columnIndex);

  Object getObject(int columnIndex);

  BigDecimal getBigDecimal(int columnIndex);
}