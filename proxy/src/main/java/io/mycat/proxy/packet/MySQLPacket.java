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
package io.mycat.proxy.packet;

import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.payload.MySQLPayloadReader;

import java.nio.ByteBuffer;


public interface MySQLPacket<T extends ProxyBuffer> extends MySQLPayloadReader {
    public final static byte[] EMPTY_BYTE_ARRAY = new byte[]{};

    public static int getPacketHeaderSize() {
        return 4;
    }

    T currentBuffer();

    public default boolean multiPackets() {
        return false;
    }

    public void reset();

    public int packetReadStartIndex();

    public int packetReadStartIndex(int index);

    public int packetReadEndIndex();

    public int packetReadEndIndex(int endPos);

    public default int packetReadStartIndexAdd(int len) {
        return packetReadStartIndex(packetReadStartIndex() + len);
    }

    public int packetWriteIndex();

    public int packetWriteIndex(int index);

    public default int skip4() {
        return packetWriteIndex(packetWriteIndex() + 4);
    }

    public default int packetWriteIndexAdd(int len) {
        return packetWriteIndex(packetWriteIndex() + len);
    }
//
//    public T appendInReading(T packet);
//    public T appendInWriting(T packet);

    /**
     * 获取lenenc占用的字节长度
     *
     * @param lenenc 值
     * @return 长度
     */
    public static int getLenencLength(int lenenc) {
        if (lenenc < 251) {
            return 1;
        } else if (lenenc >= 251 && lenenc < (1 << 16)) {
            return 3;
        } else if (lenenc >= (1 << 16) && lenenc < (1 << 24)) {
            return 4;
        } else {
            return 9;
        }
    }

    public default MySQLPacket writeBytes(byte[] bytes) {
        this.writeBytes(bytes.length, bytes);
        return this;
    }

    public default long readFixInt(int length) {
        long val = getInt(packetReadStartIndex(), length);
        packetReadStartIndexAdd(length);
        return val;
    }

    public default long getFixInt(int index, int length) {
        return getInt(index, length);
    }

    public default int readLenencInt() {
        int index = packetReadStartIndex();
        long len = getInt(index, 1) & 0xff;
        if (len < 251) {
            packetReadStartIndexAdd(1);
            return getInt(index, 1);
        } else if (len == 0xfc) {
            packetReadStartIndexAdd(3);
            return getInt(index + 1, 2);
        } else if (len == 0xfd) {
            packetReadStartIndexAdd(4);
            return getInt(index + 1, 3);
        } else {
            packetReadStartIndexAdd(9);
            return getInt(index + 1, 8);
        }
    }

    public default int getInt(int index, int length) {
        currentBuffer().position(index);
        int rv = 0;
        for (int i = 0; i < length; i++) {
            byte b = currentBuffer().get();
            rv |= (((long) b) & 0xFF) << (i * 8);
        }
        return rv;
    }
    public static int getInt(ByteBuffer buffer,int length) {
        int rv = 0;
        for (int i = 0; i < length; i++) {
            byte b = buffer.get();
            rv |= (((long) b) & 0xFF) << (i * 8);
        }
        return rv;
    }
    public default byte[] getBytes(int index, int length) {
        currentBuffer().position(index);
        byte[] bytes = new byte[length];
        currentBuffer().get(bytes);
        return bytes;
    }

    public default byte getByte(int index) {
        return currentBuffer().get(index);
    }

    public default String getFixString(int index, int length) {
        byte[] bytes = getBytes(index, length);
        return new String(bytes);
    }

    public default byte[] readFixStringBytes(int length) {
        byte[] bytes = getBytes(packetReadStartIndex(), length);
        packetReadStartIndexAdd(length);
        return bytes;
    }

    public default String readFixString(int length) {
        byte[] bytes = getBytes(packetReadStartIndex(), length);
        packetReadStartIndexAdd(length);
        return new String(bytes);
    }

    public default String getLenencString(int index) {
        int strLen = (int) getLenencInt(index);
        int lenencLen = getLenencLength(strLen);
        byte[] bytes = getBytes(index + lenencLen, strLen);
        return new String(bytes);
    }

    public default String readLenencString() {
        return new String(readLenencStringBytes());
    }

    public default byte[] readLenencStringBytes() {
        int strLen = (int) getLenencInt(packetReadStartIndex());
        int lenencLen = getLenencLength(strLen);
        byte[] bytes = getBytes(packetReadStartIndex() + lenencLen, strLen);
        packetReadStartIndexAdd(strLen + lenencLen);
        return bytes;
    }

    public default String getVarString(int index, int length) {
        return getFixString(index, length);
    }

    public default String readVarString(int length) {
        return readFixString(length);
    }

    public default String getNULString(int index) {
        return new String(getNULStringBytes(index));
    }

    public default byte[] getNULStringBytes(int index) {
        int strLength = 0;
        int scanIndex = index;
        int length = packetReadEndIndex();
        while (scanIndex < length) {
            if (getByte(scanIndex++) == 0) {
                break;
            }
            strLength++;
        }
        return getBytes(index, strLength);
    }

    public default byte[] readNULStringBytes() {
        byte[] rv = getNULStringBytes(packetReadStartIndex());
        packetReadStartIndexAdd(rv.length + 1);
        return rv;
    }

    public default String readNULString() {
        return new String(readNULStringBytes());
    }

    public default byte[] getEOFStringBytes(int index) {
        int strLength = packetReadEndIndex() - index;
        return getBytes(index, strLength);
    }

    public default byte[] readEOFStringBytes() {
        byte[] rv = getEOFStringBytes(packetReadStartIndex());
        packetReadStartIndexAdd(rv.length);
        return rv;
    }


    public default String getEOFString(int index) {
        return new String(getEOFStringBytes(index));
    }

    public default String readEOFString() {
        return new String(readEOFStringBytes());
    }


    public default MySQLPacket putFixInt(int index, int length, long val) {
        int index0 = index;
        for (int i = 0; i < length; i++) {
            byte b = (byte) ((val >>> (i * 8)) & 0xFF);
            putByte(index0++, b);
        }
        return this;
    }

    public static byte[] getFixIntByteArray(int length, long val) {
        byte[] bytes = new byte[length];
        int index = 0;
        for (int i = 0; i < length; i++) {
            byte b = (byte) ((val >>> (i * 8)) & 0xFF);
            bytes[index++] = b;
        }
        return bytes;
    }

    public static void writeFixIntByteBuffer(ByteBuffer buffer, int length, long val) {
        for (int i = 0; i < length; i++) {
            byte b = (byte) ((val >>> (i * 8)) & 0xFF);
            buffer.put(b);
        }
    }

    public default MySQLPacket writeFixInt(int length, long val) {
        putFixInt(packetWriteIndex(), length, val);
        return this;
    }

    public default int putLenencIntReLenencLen(int index, long val) {
        if (val < 251) {
            putByte(index, (byte) val);
            return 1;
        } else if (val >= 251 && val < (1 << 16)) {
            putByte(index, (byte) 0xfc);
            putFixInt(index + 1, 2, val);
            return 3;
        } else if (val >= (1 << 16) && val < (1 << 24)) {
            putByte(index, (byte) 0xfd);
            putFixInt(index + 1, 3, val);
            return 4;
        } else {
            putByte(index, (byte) 0xfe);
            putFixInt(index + 1, 8, val);
            return 9;
        }
    }

    public default MySQLPacket writeLenencInt(long val) {
        if (val < 251) {
            putByte(packetWriteIndexAdd(1), (byte) val);
        } else if (val >= 251 && val < (1 << 16)) {
            putByte(packetWriteIndexAdd(1), (byte) 0xfc);
            putFixInt(packetWriteIndex(), 2, val);
            packetWriteIndexAdd(2);
        } else if (val >= (1 << 16) && val < (1 << 24)) {
            putByte(packetWriteIndexAdd(1), (byte) 0xfd);
            putFixInt(packetWriteIndex(), 3, val);
            packetWriteIndexAdd(3);
        } else {
            putByte(packetWriteIndexAdd(1), (byte) 0xfe);
            putFixInt(packetWriteIndex(), 8, val);
            packetWriteIndexAdd(8);
        }
        return this;
    }

    public default MySQLPacket putFixString(int index, String val) {
        putBytes(index, val.getBytes());
        return this;
    }

    public default MySQLPacket putFixString(int index, byte[] val) {
        putBytes(index, val);
        return this;
    }

    public default MySQLPacket writeFixString(String val) {
        byte[] bytes = val.getBytes();
        putBytes(packetWriteIndex(), bytes);
        return this;
    }

    public default MySQLPacket writeFixString(byte[] val) {
        putBytes(packetWriteIndex(), val);
        return this;
    }

    public default MySQLPacket putLenencString(int index, String val) {
        byte[] bytes = val.getBytes();
        int lenencLen = this.putLenencIntReLenencLen(index, bytes.length);
        this.putFixString(index + lenencLen, bytes);
        return this;
    }

    public default MySQLPacket putLenencString(int index, byte[] val) {
        int lenencLen = this.putLenencIntReLenencLen(index, val.length);
        this.putFixString(index + lenencLen, val);
        return this;
    }

    public default void writeLenencBytesWithNullable(byte[] bytes) {
        byte nullVal = 0;
        if (bytes == null) {
            currentBuffer().put(nullVal);
        } else {
            writeLenencBytes(bytes);
        }
    }

    public default MySQLPacket writeLenencString(byte[] bytes) {
        putLenencString(packetWriteIndex(), bytes);
        int lenencLen = getLenencLength(bytes.length);
        return this;
    }

    public default MySQLPacket writeLenencString(String val) {
        return writeLenencString(val.getBytes());
    }

    public default MySQLPacket putBytes(int index, byte[] bytes) {
        putBytes(index, bytes, bytes.length);
        return this;
    }

    public default MySQLPacket writeBytes(byte[] bytes, int offset, int length) {
        currentBuffer().position(this.packetWriteIndex());
        currentBuffer().put(bytes, offset, length);
        return this;
    }

    public default MySQLPacket writeBytes(int index, byte[] bytes, int offset, int length) {
        currentBuffer().position(index);
        currentBuffer().put(bytes, offset, length);
        return this;
    }

    public default MySQLPacket putBytes(int index, byte[] bytes, int length) {
        currentBuffer().position(index);
        currentBuffer().put(bytes, 0, length);
        return this;
    }

    public default MySQLPacket putByte(int index, byte val) {
        currentBuffer().position(index);
        currentBuffer().put(val);
        return this;
    }

    public default MySQLPacket putNULString(int index, String val) {
        byte[] bytes = val.getBytes();
        putFixString(index, bytes);
        putByte(bytes.length + index, (byte) 0);
        return this;
    }

    public default MySQLPacket putNULString(int index, byte[] bytes) {
        putFixString(index, bytes);
        putByte(bytes.length + index, (byte) 0);
        return this;
    }

    public default MySQLPacket writeNULString(String val) {
        byte[] bytes = val.getBytes();
        putNULString(packetWriteIndex(), bytes);
        return this;
    }

    public default MySQLPacket writeNULString(byte[] vals) {
        putNULString(packetWriteIndex(), vals);

        return this;
    }

    public default MySQLPacket writeEOFString(String val) {
        byte[] bytes = val.getBytes();
        putFixString(packetWriteIndex(), bytes);

        return this;
    }

    public default MySQLPacket writeEOFStringBytes(byte[] bytes) {
        putFixString(packetWriteIndex(), bytes);
        return this;
    }

    public default byte[] readBytes(int length) {
        byte[] bytes = this.getBytes(packetReadStartIndex(), length);
        packetReadStartIndexAdd(length);
        return bytes;
    }

    public default MySQLPacket writeBytes(int length, byte[] bytes) {
        this.putBytes(packetWriteIndex(), bytes, length);
        return this;
    }


    public default MySQLPacket writeLenencBytes(byte[] bytes) {
        int offset = this.putLenencIntReLenencLen(packetWriteIndex(), bytes.length);
        putBytes(packetWriteIndex() + offset, bytes);
        return this;
    }

    public default MySQLPacket writeLenencBytes(byte[] bytes, byte[] nullValue) {
        if (bytes == null) {
            return writeLenencBytes(nullValue);
        } else {
            return writeLenencBytes(bytes);
        }
    }


    public default MySQLPacket writeByte(byte val) {
        this.putByte(packetWriteIndex(), val);
        return this;
    }

    public default MySQLPacket writeReserved(int length) {
        int i1 = packetWriteIndex();
        for (int i = 0; i < length; i++) {
            this.writeByte( (byte) 0);
        }
        return this;
    }

    public default byte readByte() {
        byte val = getByte(packetReadStartIndex());
        packetReadStartIndexAdd(1);
        return val;
    }

    public default byte[] getLenencBytes(int index) {
        int len = (int) getLenencInt(index);
        return getBytes(index + getLenencLength(len), len);
    }

    public default long getLenencInt(int index) {
        long len = getInt(index, 1) & 0xff;
        if (len == 0xfc) {
            return getInt(index + 1, 2);
        } else if (len == 0xfd) {
            return getInt(index + 1, 3);
        } else if (len == 0xfe) {
            return getInt(index + 1, 8);
        } else if (len == 0xfb) {
            return len;
        } else {
            return len;
        }
    }

    public default byte[] readLenencBytes() {
        int len = (int) getLenencInt(packetReadStartIndex());
        byte[] bytes = null;
        if ((len & 0xff) == 0xfb) {
            bytes = EMPTY_BYTE_ARRAY;
        } else {
            bytes = getBytes(packetReadStartIndex() + getLenencLength(len), len);
        }
        packetReadStartIndexAdd(getLenencLength(len) + len);
        return bytes;
    }

    public default MySQLPacket putLenencBytes(int index, byte[] bytes) {
        int offset = this.putLenencIntReLenencLen(index, bytes.length);
        putBytes(index + offset, bytes);
        return this;
    }

    public default boolean readFinished() {
        return packetReadStartIndex() == packetReadEndIndex();
    }

    public default void skipInReading(int i) {
        packetReadStartIndexAdd(i);
    }

    public default void writeSkipInWriting(int i) {
        packetWriteIndex(packetWriteIndex() + i);
    }

    public void writeFloat(float f);

    public float readFloat();

    public void writeLong(long l);

    public long readLong();

    public void writeDouble(double d);

    public double readDouble();

    void writeShort(short o);
}
