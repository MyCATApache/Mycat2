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

import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.PacketSplitter;
import io.mycat.proxy.packet.PacketSplitterImpl;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.mycat.proxy.packet.MySQLPacket.EMPTY_BYTE_ARRAY;
import static io.mycat.proxy.packet.MySQLPacket.getLenencLength;

public class MySQLPayloadReadView implements MySQLPayloadReader<MySQLPayloadReadView> {
    ByteBuffer[] buffers = new ByteBuffer[1];
    int totalLength;
    int arrayOffset;
    ProxyBuffer proxyBuffer;
    PacketSplitter packetSplitter = new PacketSplitterImpl();
    int startPos;
    int endPos;

    public void init(MySQLPacketResolver resolver) {
        startPos = resolver.getWholePacketStartPos();
        endPos = resolver.getWholePacketEndPos();
        totalLength = resolver.getPayloadLength();
        proxyBuffer = (ProxyBuffer) resolver.currentProxybuffer();
        doReadReady();
    }

    @Override
    public int length() {
        return totalLength;
    }

    public void rewindPos() {
        proxyBuffer.channelReadStartIndex(startPos);
        proxyBuffer.channelWriteEndIndex(endPos);
    }


    private void doReadReady() {
        arrayOffset = 0;
        ByteBuffer byteBuffer = proxyBuffer.currentByteBuffer();
        if (totalLength < 0xffffff) {
            buffers[0] = byteBuffer;
            byteBuffer.position(startPos+4);
            byteBuffer.limit(endPos);
            return;
        }
        int i = 0;
        int index = this.startPos;
        packetSplitter.init(totalLength);
        while (packetSplitter.nextPacketInPacketSplitter()) {
            int payloadLen = packetSplitter.getPacketLenInPacketSplitter();
            byteBuffer.position(index + 4);
            if (i == buffers.length) {
                buffers = Arrays.copyOf(buffers, buffers.length + 1);
            }
            buffers[i] = byteBuffer.slice();
            byteBuffer.limit(payloadLen);
            index += 4 + payloadLen;
        }
    }


    @Override
    public long readFixInt(int length) {
        int rv = 0;
        for (int i = 0; i < length; i++) {
            byte b = readByte();
            rv |= (((long) b) & 0xFF) << (i * 8);
        }
        return rv;
    }

    @Override
    public int readLenencInt() {
        long len = readFixInt(1) & 0xff;
        if (len < 251) {
            return (int) readFixInt(1);
        } else if (len == 0xfc) {
            return (int) readFixInt(2);
        } else if (len == 0xfd) {
            return (int) readFixInt(3);
        } else {
            return (int) readFixInt(8);
        }
    }

    @Override
    public String readFixString(int length) {
        return new String(readFixStringBytes(length));
    }

    @Override
    public String readLenencString() {
        return new String(readLenencBytes());
    }

    @Override
    public byte[] readLenencStringBytes() {
        int strLen = readLenencInt();
        int lenencLength = getLenencLength(strLen);
        return readBytes(lenencLength);
    }


    @Override
    public byte[] readNULStringBytes() {
        int len = 0;
        boolean b = true;
        for (int i = arrayOffset; i < buffers.length && b; i++) {
            ByteBuffer buffer1 = buffers[i];
            int position = buffer1.position();
            int limit = buffer1.limit();
            for (int j = position; j < limit; j++) {
                if (buffer1.get(j) == 0) {
                    b = false;
                    len += 1;
                    break;
                } else {
                    len += 1;
                }

            }
        }
        b = true;
        byte[] bytes = new byte[len];
        int index = 0;
        for (int i = arrayOffset; i < buffers.length && b; i++) {
            ByteBuffer buffer1 = buffers[i];
            while (buffer1.hasRemaining()) {
                bytes[index++] = buffer1.get();
                if (index == len) {
                    b = false;
                    buffer1.get();
                    break;
                }
            }
        }
        return bytes;
    }

    @Override
    public String readNULString() {
        return new String(readNULStringBytes());
    }

    @Override
    public byte[] readEOFStringBytes() {
        int eofLength = 0;
        for (int i = arrayOffset; i < buffers.length; i++) {
            ByteBuffer buffer = buffers[i];
            eofLength += buffer.limit() - buffer.position();
        }
        byte[] bytes = new byte[eofLength];
        int offset = 0;
        for (int i = arrayOffset; i < buffers.length; i++) {
            ByteBuffer buffer = buffers[i];
            int cLength = buffer.limit() - buffer.position();
            buffer.get(bytes, offset, cLength);
            offset += cLength;
        }
        return bytes;
    }

    @Override
    public String readEOFString() {
        return new String(readEOFStringBytes());
    }

    @Override
    public byte[] readBytes(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = readByte();
        }
        return bytes;
    }

    @Override
    public byte[] readFixStringBytes(int length) {
        return readBytes(length);
    }

    @Override
    public byte readByte() {
        ByteBuffer buffer = buffers[arrayOffset];
        byte b = buffer.get();
        if (!buffer.hasRemaining()) {
            ++arrayOffset;
        }
        return b;
    }

    @Override
    public byte[] readLenencBytes() {
        int len = readLenencInt();
        if ((len & 0xff) == 0xfb) {
            return EMPTY_BYTE_ARRAY;
        } else {
            return readBytes(len);
        }
    }


    @Override
    public long readLong() {
        byte b0 = readByte();
        byte b1 = readByte();
        byte b2 = readByte();
        byte b3 = readByte();
        byte b4 = readByte();
        byte b5 = readByte();
        byte b6 = readByte();
        byte b7 = readByte();
        return makeLong(b7, b6, b5, b4, b3, b2, b1, b0);
    }


    @Override
    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public void reset() {
        if (buffers.length > 1) {
            buffers = new ByteBuffer[1];
        }
    }

    @Override
    public void skipInReading(int i) {
        readByte();
    }

    @Override
    public boolean readFinished() {
        return !buffers[buffers.length-1].hasRemaining();
    }

    static private long makeLong(byte b7, byte b6, byte b5, byte b4,
                                 byte b3, byte b2, byte b1, byte b0) {
        return ((((long) b7) << 56) |
                (((long) b6 & 0xff) << 48) |
                (((long) b5 & 0xff) << 40) |
                (((long) b4 & 0xff) << 32) |
                (((long) b3 & 0xff) << 24) |
                (((long) b2 & 0xff) << 16) |
                (((long) b1 & 0xff) << 8) |
                (((long) b0 & 0xff)));
    }
}
