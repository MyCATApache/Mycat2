/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.payload;

public interface MySQLPayload<T extends MySQLPayload<T>> {
    int getLength();

//    public boolean readFromChannel(SocketChannel channel);
//    public boolean writeProxyBufferToChannel(SocketChannel channel) throws IOException;

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
