package io.mycat.proxy.payload;

public interface MySQLPayloadReader<T extends MySQLPayloadReader<T>> {
    int length();
    public long readFixInt(int length);
    public  int readLenencInt();
    public  String readFixString(int length);
    public  String readLenencString();
    public  byte[] readLenencStringBytes();
    public  byte[] readNULStringBytes();
    public  String readNULString();
    public  byte[] readEOFStringBytes();
    public  String readEOFString();
    public  byte[] readBytes(int length);
    public byte[] readFixStringBytes(int length);
    public  byte readByte();
    public  byte[] readLenencBytes();
    public long readLong();
    public double readDouble();
    public void reset();

    void skipInReading(int i);

    boolean readFinished();
}
