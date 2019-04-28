package io.mycat.proxy.payload;

public interface MySQLPayload<T extends MySQLPayload<T>> {
    int getLength();

//    public boolean readFromChannel(SocketChannel channel);
//    public boolean writeToChannel(SocketChannel channel) throws IOException;

    byte get();

    public MySQLPayload writeBytes(byte[] bytes);
    public long readFixInt(int length);
    public  int readLenencInt();
    public  String readFixString(int length);
    public  String readLenencString();
    public  byte[] readLenencStringBytes();
    public  String readVarString(int length);
    public  byte[] readNULStringBytes();
    public  String readNULString();
    public  byte[] readEOFStringBytes();
    public  String readEOFString();
    public  byte[] readBytes(int length);
    public  byte readByte();
    public  byte[] readLenencBytes();
    public  boolean readFinished();
    public float readFloat();
    public long readLong();
    public double readDouble();

    public MySQLPayload writeFloat(float f);
    public MySQLPayload writeLong(long l);
    public  MySQLPayload writeFixInt(int length, long val);
    public  MySQLPayload writeLenencInt(long val);
    public  MySQLPayload writeFixString(String val);
    public  MySQLPayload writeFixString(byte[] val);
    public  MySQLPayload writeLenencBytesWithNullable(byte[] bytes);
    public  MySQLPayload writeLenencString(byte[] bytes);
    public  MySQLPayload writeLenencString(String val);
    public  MySQLPayload writeVarString(String val);
    public  MySQLPayload writeBytes(byte[] bytes, int offset, int length);
    public  MySQLPayload writeNULString(String val);
    public  MySQLPayload writeNULString(byte[] vals);
    public  MySQLPayload writeEOFString(String val);
    public  MySQLPayload writeEOFStringBytes(byte[] bytes);
    public  MySQLPayload writeBytes(int length, byte[] bytes);
    public  MySQLPayload writeLenencBytes(byte[] bytes);
    public  MySQLPayload writeLenencBytes(byte[] bytes, byte[] nullValue);
    public  MySQLPayload writeByte(byte val);
    public  MySQLPayload writeReserved(int length);
    public MySQLPayload writeDouble(double d);
    MySQLPayload writeShort(short o);


    void skip(int i);
}
