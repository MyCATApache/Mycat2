/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans.mysql.packet;


import io.mycat.MycatException;
import io.mycat.config.MySQLServerCapabilityFlags;

/**
 * cjw
 * 294712221@qq.com
 * 作为客户端发送的握手包,是服务器发送的验证包的响应
 */
public class HandshakePacket {

    private int protocolVersion;
    private String serverVersion;
    private long connectionId;
    private String authPluginDataPartOne;//salt auth plugin sql part 1
    private MySQLServerCapabilityFlags capabilities;
    private boolean hasPartTwo = false;
    private int characterSet;
    private int statusFlags;
    private int authPluginDataLen;
    private String authPluginDataPartTwo;
    private String authPluginName;

    public void readPayload(MySQLPayloadReadView buffer) {
        protocolVersion = buffer.readByte();
        if (protocolVersion != 0x0a) {
            throw new MycatException("unsupport HandshakeV9");
        }
        serverVersion = buffer.readNULString();
        connectionId = buffer.readFixInt(4);
        authPluginDataPartOne = buffer.readFixString(8);
        buffer.skipInReading(1);
        capabilities = new MySQLServerCapabilityFlags((int) buffer.readFixInt(2) & 0x0000ffff);
        if (!buffer.readFinished()) {
            hasPartTwo = true;
            characterSet = buffer.readByte();
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
            throw new MycatException("authPluginDataPartOne's length must be 8!");
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

    public int getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(int protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public void setServerVersion(String serverVersion) {
        this.serverVersion = serverVersion;
    }

    public long getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(long connectionId) {
        this.connectionId = connectionId;
    }

    public String getAuthPluginDataPartOne() {
        return authPluginDataPartOne;
    }

    public void setAuthPluginDataPartOne(String authPluginDataPartOne) {
        this.authPluginDataPartOne = authPluginDataPartOne;
    }

    public MySQLServerCapabilityFlags getCapabilities() {
        return capabilities;
    }

    public void setCapabilities(MySQLServerCapabilityFlags capabilities) {
        this.capabilities = capabilities;
    }

    public boolean isHasPartTwo() {
        return hasPartTwo;
    }

    public void setHasPartTwo(boolean hasPartTwo) {
        this.hasPartTwo = hasPartTwo;
    }

    public int getCharacterSet() {
        return characterSet;
    }

    public void setCharacterSet(int characterSet) {
        this.characterSet = characterSet;
    }

    public int getStatusFlags() {
        return statusFlags;
    }

    public void setStatusFlags(int statusFlags) {
        this.statusFlags = statusFlags;
    }

    public int getAuthPluginDataLen() {
        return authPluginDataLen;
    }

    public void setAuthPluginDataLen(int authPluginDataLen) {
        this.authPluginDataLen = authPluginDataLen;
    }

    public String getAuthPluginDataPartTwo() {
        return authPluginDataPartTwo;
    }

    public void setAuthPluginDataPartTwo(String authPluginDataPartTwo) {
        this.authPluginDataPartTwo = authPluginDataPartTwo;
    }

    public String getAuthPluginName() {
        return authPluginName;
    }

    public void setAuthPluginName(String authPluginName) {
        this.authPluginName = authPluginName;
    }
}
