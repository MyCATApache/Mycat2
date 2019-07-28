package io.mycat.datasource.jdbc;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

public enum TextConvertorImpl implements TextConvertor {
  INSANTCE;

  @Override
  public byte[] convertBigDecimal(BigDecimal v) {
    return v.toPlainString().getBytes();
  }

  @Override
  public byte[] convertBoolean(boolean v) {
    return String.valueOf(v).getBytes();
  }

  @Override
  public byte[] convertByte(byte v) {
    return String.valueOf(v).getBytes();
  }

  @Override
  public byte[] convertShort(short v) {
    return String.valueOf(v).getBytes();
  }

  @Override
  public byte[] convertInteger(int v) {
    return String.valueOf(v).getBytes();
  }

  @Override
  public byte[] convertLong(long v) {
    return String.valueOf(v).getBytes();
  }

  @Override
  public byte[] convertFloat(float v) {
    return String.valueOf(v).getBytes();
  }

  @Override
  public byte[] convertDouble(double v) {
    return String.valueOf(v).getBytes();
  }

  @Override
  public byte[] convertBytes(byte[] v) {
    return v;
  }

  @Override
  public byte[] convertDate(Date v) {
    return v.toString().getBytes();
  }

  @Override
  public byte[] convertTime(Time v) {
    return v.toString().getBytes();
  }

  @Override
  public byte[] convertTimeStamp(Timestamp v) {
    return v.toString().getBytes();
  }

  @Override
  public byte[] convertBlob(Blob v) {
    try {
      return v.getBytes(0, (int) v.length());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte[] convertClob(Clob v) {
    return v.toString().getBytes();
  }

  @Override
  public byte[] convertObject(Object v) {
    if (v == null) {
      return null;
    }
    if (v instanceof byte[]) {
      return (byte[]) v;
    }
    return v.toString().getBytes();
  }
}