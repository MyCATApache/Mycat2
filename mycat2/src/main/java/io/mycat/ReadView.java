package io.mycat;

import io.mycat.beans.mysql.packet.MySQLPayloadReadView;
import io.vertx.core.buffer.Buffer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ReadView implements MySQLPayloadReadView {
    int index = 0;

    public ReadView(Buffer buffer) {
        this.buffer = buffer;
    }

    final Buffer buffer;

    @Override
    public int length() {
        return this.buffer.length();
    }

    @Override
    public long readFixInt(int length) {
        int rv = 0;
        for (int i = 0; i < length; i++) {
            byte b = buffer.getByte(index + i);
            rv |= (((long) b) & 0xFF) << (i * 8);
        }
        index += length;
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
        int tmp = index;
        index += length;
        return buffer.getString(tmp, tmp + length);
    }

    @Override
    public String readLenencString() {
        return new String(readLenencStringBytes(), UTF_8);
    }

    @Override
    public byte[] readLenencStringBytes() {
        return readLenencBytes();
    }

    @Override
    public byte[] readNULStringBytes() {
        int strLength = 0;
        int scanIndex = index;
        int length = length();
        while (scanIndex < length) {
            if (buffer.getByte(scanIndex++) == 0) {
                break;
            }
            strLength++;
        }
        int tmp = index;
        index += (strLength + 1);
        return buffer.getBytes(tmp, tmp + strLength);
    }

    @Override
    public String readNULString() {
        return new String(readNULStringBytes(), UTF_8);
    }

    @Override
    public byte[] readEOFStringBytes() {
        return readBytes(length() - index);
    }

    @Override
    public String readEOFString() {
        return new String(readEOFStringBytes(), UTF_8);
    }

    @Override
    public byte[] readBytes(int length) {
        int tmp = index;
        return buffer.getBytes(tmp, index = index + length);
    }

    @Override
    public byte[] readFixStringBytes(int length) {
        return readBytes(length);
    }

    @Override
    public byte readByte() {
        int tmp = index;
        index += 1;
        return buffer.getByte(tmp);
    }

    @Override
    public byte[] readLenencBytes() {
        int len = readLenencInt();
        byte[] bytes = null;
        if ((len & 0xff) == 0xfb) {
            return null;
        } else {
            bytes = readBytes(len);
        }
        return bytes;
    }

    @Override
    public long readLong() {
        int tmp = index;
        index+=8;
        return buffer.getLong(tmp);
    }

    @Override
    public double readDouble() {
        int tmp = index;
        index+=8;
        return buffer.getDouble(tmp);
    }

    @Override
    public void reset() {
        index = 0;
    }

    @Override
    public void skipInReading(int i) {
        index += i;
    }

    @Override
    public boolean readFinished() {
        return index >= this.buffer.length();
    }

    @Override
    public float readFloat() {
        int tmp = index;
        index+=4;
        return  buffer.getFloat(tmp);
    }
}