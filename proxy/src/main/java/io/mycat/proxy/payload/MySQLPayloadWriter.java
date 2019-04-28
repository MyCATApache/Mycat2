package io.mycat.proxy.payload;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public interface MySQLPayloadWriter<T extends MySQLPayloadWriter<T>> {
    public T writeLong(long l);

    public T writeFixInt(int length, long val);

    public T writeLenencInt(long val);

    public T writeFixString(String val);

    public T writeFixString(byte[] val);

    public T writeLenencBytesWithNullable(byte[] bytes);

    public T writeLenencString(byte[] bytes);

    public T writeLenencString(String val);

    public T writeBytes(byte[] bytes, int offset, int length);

    public T writeNULString(String val);

    public T writeNULString(byte[] vals);

    public T writeEOFString(String val);

    public T writeEOFStringBytes(byte[] bytes);

    public T writeLenencBytes(byte[] bytes);

    public T writeLenencBytes(byte[] bytes, byte[] nullValue);

    public T writeByte(byte val);

    default public T writeByte(int val){
        return writeByte((byte)val);
    }

    public T writeReserved(int length);

    public T writeDouble(double d);


    public boolean writeToChannel(SocketChannel channel) throws IOException;

    public int startPacket();

    public int startPacket(int payload);

    public int endPacket();

    public void setPacketId(int packetId);

    public byte increaseAndGetPacketId();
}
