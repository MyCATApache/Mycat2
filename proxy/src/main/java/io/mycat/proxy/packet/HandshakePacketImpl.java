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


import io.mycat.MycatExpection;
import io.mycat.beans.mysql.packet.MySQLPayloadReadView;
import io.mycat.beans.mysql.packet.MySQLPayloadWriteView;
import io.mycat.config.MySQLServerCapabilityFlags;

/**
 * cjw
 * 294712221@qq.com
 * 作为客户端发送的握手包,是服务器发送的验证包的响应
 */
public class HandshakePacketImpl {
    public int protocolVersion;
    public String serverVersion;
    public long connectionId;
    public String authPluginDataPartOne;//salt auth plugin sql part 1
    public MySQLServerCapabilityFlags capabilities;
    public boolean hasPartTwo = false;
    public int characterSet;
    public int statusFlags;
    public int authPluginDataLen;
    public String authPluginDataPartTwo;
    public String authPluginName;

  public void readPayload(MySQLPayloadReadView buffer) {
        protocolVersion = buffer.readByte();
        if (protocolVersion != 0x0a) {
            throw new MycatExpection("unsupport HandshakeV9");
        }
        serverVersion = buffer.readNULString();
        connectionId = buffer.readFixInt(4);
        authPluginDataPartOne = buffer.readFixString(8);
        buffer.skipInReading(1);
        capabilities = new MySQLServerCapabilityFlags((int) buffer.readFixInt(2) & 0x0000ffff);
        if (!buffer.readFinished()) {
            hasPartTwo = true;
            characterSet = Byte.toUnsignedInt(buffer.readByte());
            statusFlags = (int) buffer.readFixInt(2);
            long l = buffer.readFixInt(2) << 16;
            capabilities.value |= l;
            if (capabilities.isPluginAuth()) {
                byte b = buffer.readByte();
                authPluginDataLen = Byte.toUnsignedInt(b);
            } else {
                buffer.skipInReading(1);
            }
            //reserved = buffers.readFixString(10);
         buffer.skipInReading(10);
            if (capabilities.isCanDo41Anthentication()) {
                //todo check length in range [13.authPluginDataLen)
//                authPluginDataPartTwo = buffers.readFixString(13);
                authPluginDataPartTwo = buffer.readNULString();
            }
            if (capabilities.isPluginAuth()) {
                authPluginName = buffer.readNULString();
            }
        }
    }

  public void writePayload(MySQLPayloadWriteView buffer) {
        buffer.writeByte((byte) 0x0a);
        buffer.writeNULString(serverVersion);
        buffer.writeFixInt(4, connectionId);
        if (authPluginDataPartOne.length() != 8) {
          throw new MycatExpection("authPluginDataPartOne's length must be 8!");
        }
        buffer.writeFixString(authPluginDataPartOne);
        buffer.writeByte((byte) 0);
        buffer.writeFixInt(2, this.capabilities.getLower2Bytes());
        if (hasPartTwo) {
            buffer.writeByte((byte) characterSet);
            buffer.writeFixInt(2, this.statusFlags);
            buffer.writeFixInt(2, this.capabilities.getUpper2Bytes());
            if (this.capabilities.isPluginAuth()) {
                buffer.writeByte((byte) authPluginDataLen);
            } else {
                buffer.writeByte((byte) 0);
            }
            buffer.writeReserved(10);
            if (capabilities.isCanDo41Anthentication()) {
                //todo check length in range [13.authPluginDataLen)
//                buffers.writeFixString(authPluginDataPartTwo);
                buffer.writeNULString(authPluginDataPartTwo);
            }
            if (capabilities.isPluginAuth()) {
                buffer.writeNULString(this.authPluginName);
            }
        }
    }

    public int calcPacketSize() {
//        buffers.writeByte((byte) 0x0a);
//        buffers.writeNULString(serverVersion);
//        buffers.writeFixInt(4, connectionId);
        int size = 0;
        size += 1 + serverVersion.length() + 1 + 4;
//        buffers.writeFixString(authPluginDataPartOne);
//        buffers.writeByte((byte) 0);
//        buffers.writeFixInt(2, this.getCapabilities.getLower2Bytes());
        size += 8 + 1 + 2;
        if (hasPartTwo) {
//            buffers.writeByte((byte) characterSet);
//            buffers.writeFixInt(2, this.statusFlags);
//            buffers.writeFixInt(2, this.getCapabilities.getUpper2Bytes());
            size += 1 + 2 + 2;
//            if (this.getCapabilities.isPluginAuth()) {
//                buffers.writeByte((byte) authPluginDataLen);
//            } else {
//                buffers.writeByte((byte) 0);
//            }
            size += 1;
//            buffers.writeReserved(10);
            size += 10;
            if (capabilities.isCanDo41Anthentication()) {
                //todo check length in range [13.authPluginDataLen)
//                buffers.writeFixString(authPluginDataPartTwo);
                size += authPluginDataPartTwo.length() + 1;
            }
            if (capabilities.isPluginAuth()) {
//                buffers.writeNULString(this.authPluginName);
                size += this.authPluginName.length() + 1;
            }
        }
        return size;
    }
}
