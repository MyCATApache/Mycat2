package io.mycat.proxy.payload;

import io.mycat.util.ByteArrayOutput;

/**
 * @author jamie12221
 * @date 2019-05-07 21:47
 **/
public class MySQLPacketWriter extends ByteArrayOutput implements MySQLPayloadWriter<MySQLPacketWriter> {

  public MySQLPacketWriter() {
  }

  public MySQLPacketWriter(int size) {
    super(size);
  }

  @Override
  public MySQLPacketWriter writeLong(long x) {
    writeByte(long0(x));
    writeByte(long1(x));
    writeByte(long2(x));
    writeByte(long3(x));
    writeByte(long4(x));
    writeByte(long5(x));
    writeByte(long6(x));
    writeByte(long7(x));
    return this;
  }

  @Override
  public MySQLPacketWriter writeFixInt(int length, long val) {
    for (int i = 0; i < length; i++) {
      byte b = (byte) ((val >>> (i * 8)) & 0xFF);
      writeByte(b);
    }
    return this;
  }

  @Override
  public MySQLPacketWriter writeLenencInt(long val) {
    if (val < 251) {
      writeByte((byte) val);
    } else if (val >= 251 && val < (1 << 16)) {
      writeByte((byte) 0xfc);
      writeFixInt(2,val);
    } else if (val >= (1 << 16) && val < (1 << 24)) {
      writeByte((byte) 0xfd);
      writeFixInt(3,val);
    } else {
      writeByte((byte) 0xfe);
      writeFixInt(8,val);
    }
    return this;
  }

  @Override
  public MySQLPacketWriter writeFixString(String val) {
    writeFixString(val.getBytes());
    return this;
  }

  @Override
  public MySQLPacketWriter writeFixString(byte[] bytes) {
    writeBytes(bytes, 0, bytes.length);
    return this;
  }

  @Override
  public MySQLPacketWriter writeLenencBytesWithNullable(byte[] bytes) {
    byte nullVal = 0;
    if (bytes == null) {
      writeByte(nullVal);
    } else {
      writeLenencBytes(bytes);
    }
    return this;
  }

  @Override
  public MySQLPacketWriter writeLenencString(byte[] bytes) {
    return writeLenencBytes(bytes);
  }

  @Override
  public MySQLPacketWriter writeLenencString(String val) {
    return writeLenencBytes(val.getBytes());
  }
  public MySQLPacketWriter writeBytes(byte[] bytes) {
    write(bytes, 0, bytes.length);
    return this;
  }
  @Override
  public MySQLPacketWriter writeBytes(byte[] bytes, int offset, int length) {
    write(bytes, offset, length);
    return this;
  }


  @Override
  public MySQLPacketWriter writeNULString(String val) {
    return writeNULString(val.getBytes());
  }

  @Override
  public MySQLPacketWriter writeNULString(byte[] vals) {
    writeFixString(vals);
    writeByte(0);
    return this;
  }

  @Override
  public MySQLPacketWriter writeEOFString(String val) {
    return writeFixString(val);
  }

  @Override
  public MySQLPacketWriter writeEOFStringBytes(byte[] bytes) {
    return writeBytes(bytes, 0, bytes.length);
  }

  @Override
  public MySQLPacketWriter writeLenencBytes(byte[] bytes) {
    writeLenencInt(bytes.length);
    writeBytes(bytes);
    return this;
  }

  @Override
  public MySQLPacketWriter writeLenencBytes(byte[] bytes, byte[] nullValue) {
    if (bytes == null) {
      return writeLenencBytes(nullValue);
    } else {
      return writeLenencBytes(bytes);
    }
  }

  @Override
  public MySQLPacketWriter writeByte(byte val) {
    write(val);
    return this;
  }

  @Override
  public MySQLPacketWriter writeReserved(int length) {
    for (int i = 0; i < length; i++) {
      writeByte(0);
    }
    return this;
  }

  @Override
  public MySQLPacketWriter writeDouble(double d) {
    writeLong(Double.doubleToRawLongBits(d));
    return this;
  }

  private static byte long7(long x) {
    return (byte) (x >> 56);
  }

  private static byte long6(long x) {
    return (byte) (x >> 48);
  }

  private static byte long5(long x) {
    return (byte) (x >> 40);
  }

  private static byte long4(long x) {
    return (byte) (x >> 32);
  }

  private static byte long3(long x) {
    return (byte) (x >> 24);
  }

  private static byte long2(long x) {
    return (byte) (x >> 16);
  }

  private static byte long1(long x) {
    return (byte) (x >> 8);
  }

  private static byte long0(long x) {
    return (byte) (x);
  }
}
