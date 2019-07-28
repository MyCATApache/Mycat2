package io.mycat.datasource.jdbc;

import java.math.BigDecimal;

public interface TextConvertor {

  byte[] convertBigDecimal(BigDecimal v);

  byte[] convertBoolean(boolean v);

  byte[] convertByte(byte v);

  byte[] convertShort(short v);

  byte[] convertInteger(int v);

  byte[] convertLong(long v);

  byte[] convertFloat(float v);

  byte[] convertDouble(double v);

  byte[] convertBytes(byte[] v);

  byte[] convertDate(java.sql.Date v);

  byte[] convertTime(java.sql.Time v);

  byte[] convertTimeStamp(java.sql.Timestamp v);

  byte[] convertBlob(java.sql.Blob v);

  byte[] convertClob(java.sql.Clob v);

  byte[] convertObject(Object v);
}