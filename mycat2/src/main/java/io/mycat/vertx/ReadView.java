/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.vertx;

import io.mycat.beans.mysql.packet.MySQLPayloadReadView;
import io.vertx.core.buffer.Buffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
    public Long readLenencInt() {
        int len = buffer.getByte(index)&0xff;
        index++;
        if (len < 251) {
            return  Long.valueOf(len);
        } else if (len == 0xfc) {
            return  readFixInt(2);
        } else if (len == 0xfd) {
            return Long.valueOf(readFixInt(3));
        } else if (len == 0xfb) {
            return null;
        } else {
            return Long.valueOf(readFixInt(8));
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
        byte[] bytes = readLenencStringBytes();
        if (bytes == null)return null;
        return new String(bytes, UTF_8);
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
        Long aLong = readLenencInt();
        if (aLong == null){
            return null;
        }
        int len =aLong.intValue();
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
        return buffer.getLongLE(tmp);
    }

    @Override
    public double readDouble() {
        int tmp = index;
        index+=8;
        byte[] bytes = buffer.getBytes(tmp, index);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getDouble();
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
        byte[] bytes = buffer.getBytes(tmp, index);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getFloat();
    }
}