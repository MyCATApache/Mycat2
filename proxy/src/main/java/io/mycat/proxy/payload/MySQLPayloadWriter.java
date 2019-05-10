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


  //  public boolean writeProxyBufferToChannel(SocketChannel channel) throws IOException;

//    public int startPacket();
//
//    public int startPacket(int payload);
//
//    public int endPacket();
//
//    public void setPacketId(int packetId);
//
//    public byte increaseAndGetPacketId();
}
