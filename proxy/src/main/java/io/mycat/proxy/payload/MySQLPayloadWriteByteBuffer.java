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

import io.mycat.proxy.buffer.BufferPool;
import io.mycat.proxy.packet.MySQLPacket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class MySQLPayloadWriteByteBuffer implements MySQLPayloadWriter<MySQLPayloadWriteByteBuffer> {
    ByteBuffer[] buffers = new ByteBuffer[1];
    int length;
    int totalLength;
    int bufferLength;
    byte packetId;
    BufferPool bufferPool;
    int arrayOffset;
    ByteBuffer header;
    int headerStartPos = 0;

    @Override
    public MySQLPayloadWriteByteBuffer writeLong(long x) {
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
    public MySQLPayloadWriteByteBuffer writeFixInt(int length, long val) {
        for (int i = 0; i < length; i++) {
            byte b = (byte) ((val >>> (i * 8)) & 0xFF);
            writeByte(b);
        }
        return this;
    }

    @Override
    public MySQLPayloadWriteByteBuffer writeLenencInt(long val) {
        if (val < 251) {
            writeByte((byte) val);
        } else if (val >= 251 && val < (1 << 16)) {
            writeByte((byte) 0xfc);
        } else if (val >= (1 << 16) && val < (1 << 24)) {
            writeByte((byte) 0xfd);
        } else {
            writeByte((byte) 0xfe);
        }
        return this;
    }

    @Override
    public MySQLPayloadWriteByteBuffer writeFixString(String val) {
        writeFixString(val.getBytes());
        return this;
    }

    @Override
    public MySQLPayloadWriteByteBuffer writeFixString(byte[] bytes) {
        writeBytes(bytes, 0, bytes.length);
        return this;
    }

    @Override
    public MySQLPayloadWriteByteBuffer writeLenencBytesWithNullable(byte[] bytes) {
        byte nullVal = 0;
        if (bytes == null) {
            writeByte(nullVal);
        } else {
            writeLenencBytes(bytes);
        }
        return this;
    }

    @Override
    public MySQLPayloadWriteByteBuffer writeLenencString(byte[] bytes) {
        return writeLenencBytes(bytes);
    }

    @Override
    public MySQLPayloadWriteByteBuffer writeLenencString(String val) {
        return writeLenencBytes(val.getBytes());
    }


    @Override
    public MySQLPayloadWriteByteBuffer writeBytes(byte[] bytes, int offset, int length) {
        ByteBuffer buffer = buffers[arrayOffset];
        int remains = buffer.limit() - buffer.position();
        if (remains > length) {
            buffer.put(bytes, offset, length);
            this.length += length;
        } else {
            int size = offset + length;
            for (int j = offset; j < size; j++) {
                writeByte(bytes[j]);
            }
        }
        return this;
    }

    @Override
    public MySQLPayloadWriteByteBuffer writeNULString(String val) {
        return writeNULString(val.getBytes());
    }

    @Override
    public MySQLPayloadWriteByteBuffer writeNULString(byte[] vals) {
        writeFixString(vals);
        writeByte(0);
        return this;
    }

    @Override
    public MySQLPayloadWriteByteBuffer writeEOFString(String val) {
        return writeFixString(val);
    }

    @Override
    public MySQLPayloadWriteByteBuffer writeEOFStringBytes(byte[] bytes) {
        return writeBytes(bytes, 0, bytes.length);
    }

    @Override
    public MySQLPayloadWriteByteBuffer writeLenencBytes(byte[] bytes) {
        writeLenencInt(bytes.length);
        writeLenencBytes(bytes);
        return null;
    }

    @Override
    public MySQLPayloadWriteByteBuffer writeLenencBytes(byte[] bytes, byte[] nullValue) {
        if (bytes == null) {
            return writeLenencBytes(nullValue);
        } else {
            return writeLenencBytes(bytes);
        }
    }

    @Override
    public MySQLPayloadWriteByteBuffer writeByte(byte val) {
        ByteBuffer buffer = buffers[arrayOffset];
        buffer.put((byte) val);
        if (!buffer.hasRemaining()) {
            buffer = buffers[++arrayOffset] = bufferPool.allocate();
        }
        this.length += 1;
        return this;
    }

    @Override
    public MySQLPayloadWriteByteBuffer writeReserved(int length) {
        for (int i = 0; i < length; i++) {
            writeByte(0);
        }
        return this;
    }

    @Override
    public MySQLPayloadWriteByteBuffer writeDouble(double d) {
        writeLong(Double.doubleToRawLongBits(d));
        return this;
    }

    @Override
    public boolean writeToChannel(SocketChannel channel) throws IOException {
        channel.write(buffers);
        return !buffers[buffers.length-1].hasRemaining();
    }

    @Override
    public int startPacket() {
        ByteBuffer buffer = buffers[arrayOffset];
        header = buffer;
        headerStartPos = buffer.position();
        writeByte(0);
        writeByte(0);
        writeByte(0);
        writeByte(increaseAndGetPacketId());
        return getReamins();
    }

    int getReamins() {
        return bufferLength - totalLength;
    }

    @Override
    public int startPacket(int payload) {
        return bufferLength - totalLength;
    }

    @Override
    public int endPacket() {
        int bpPosition = header.position();
        header.position(headerStartPos);
        MySQLPacket.writeFixIntByteBuffer(header, 3, totalLength);
        header.position(bpPosition);
        return getReamins();
    }

    public void doWriteReady() {
        for (ByteBuffer buffer : buffers) {
            buffer.flip();
        }
    }

    public void setPacketId(int packetId) {
        this.packetId = (byte) packetId;
    }


    public byte increaseAndGetPacketId() {
        return ++this.packetId;
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
