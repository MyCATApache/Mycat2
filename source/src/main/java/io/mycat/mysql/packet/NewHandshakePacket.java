package io.mycat.mysql.packet;

import io.mycat.mysql.CapabilityFlags;
import io.mycat.proxy.ProxyBuffer;

/**
 * cjw
 * 294712221@qq.com
 */
public class NewHandshakePacket {
    public byte packetId;
    public int protocolVersion;
    public String serverVersion;
    public long connectionId;
    public String authPluginDataPartOne;//salt auth plugin data part 1
    public CapabilityFlags capabilities;
    public boolean hasPartTwo = false;
    public int characterSet;
    public int statusFlags;
    public int authPluginDataLen;
    // public String reserved="";
    public String authPluginDataPartTwo;
    public String authPluginName;


    public void readPayload(ProxyBuffer buffer) {
        protocolVersion = buffer.readByte();
        if (protocolVersion != 0x0a) {
            throw new RuntimeException("unsupport HandshakeV9");
        }
        serverVersion = buffer.readNULString();
        connectionId = buffer.readFixInt(4);
        authPluginDataPartOne = buffer.readFixString(8);
        buffer.skip(1);
        capabilities = new CapabilityFlags((int) buffer.readFixInt(2) & 0x0000ffff);
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
                buffer.skip(1);
            }
            //reserved = buffer.readFixString(10);
            buffer.skip(10);
            if (capabilities.isCanDo41Anthentication()) {
                //todo check length in range [13.authPluginDataLen)
                authPluginDataPartTwo = buffer.readFixString(13);
            }
            if (capabilities.isPluginAuth()) {
                authPluginName = buffer.readNULString();
            }
        }
    }

    public void writePayload(ProxyBuffer buffer) {
        buffer.writeByte((byte) 0x0a);
        buffer.writeNULString(serverVersion);
        buffer.writeFixInt(4, connectionId);
        if (authPluginDataPartOne.length() != 8) {
            throw new RuntimeException("authPluginDataPartOne's length must be 8!");
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
                buffer.writeFixString(authPluginDataPartTwo);
            }
            if (capabilities.isPluginAuth()) {
                buffer.writeNULString(this.authPluginName);
            }
        }
    }

    public int calcPacketSize() {
//        buffer.writeByte((byte) 0x0a);
//        buffer.writeNULString(serverVersion);
//        buffer.writeFixInt(4, connectionId);
        int size = 0;
        size += 1 + serverVersion.length() + 1 + 4;
//        buffer.writeFixString(authPluginDataPartOne);
//        buffer.writeByte((byte) 0);
//        buffer.writeFixInt(2, this.capabilities.getLower2Bytes());
        size += 8 + 1 + 2;
        if (hasPartTwo) {
//            buffer.writeByte((byte) characterSet);
//            buffer.writeFixInt(2, this.statusFlags);
//            buffer.writeFixInt(2, this.capabilities.getUpper2Bytes());
            size += 1 + 2 + 2;
//            if (this.capabilities.isPluginAuth()) {
//                buffer.writeByte((byte) authPluginDataLen);
//            } else {
//                buffer.writeByte((byte) 0);
//            }
            size += 1;
//            buffer.writeReserved(10);
            size += 10;
            if (capabilities.isCanDo41Anthentication()) {
                //todo check length in range [13.authPluginDataLen)
//                buffer.writeFixString(authPluginDataPartTwo);
                size += authPluginDataPartTwo.length();
            }
            if (capabilities.isPluginAuth()) {
//                buffer.writeNULString(this.authPluginName);
                size += this.authPluginName.length() + 1;
            }
        }
        return size;
    }

    public void read(ProxyBuffer buffer) {
        int packetLength = (int) buffer.readFixInt(3);
        int packetId = buffer.readByte();
        readPayload(buffer);
    }
    public void write(ProxyBuffer buffer) {
        int pkgSize = calcPacketSize();
        buffer.writeFixInt(3, pkgSize);
        buffer.writeByte(packetId);
        writePayload(buffer);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NewHandshakePacket{");
        sb.append("packetId=").append(packetId);
        sb.append(", protocolVersion=").append(protocolVersion);
        sb.append(", serverVersion='").append(serverVersion).append('\'');
        sb.append(", connectionId=").append(connectionId);
        sb.append(", authPluginDataPartOne='").append(authPluginDataPartOne).append('\'');
        sb.append(", capabilities=").append(capabilities);
        sb.append(", hasPartTwo=").append(hasPartTwo);
        sb.append(", characterSet=").append(characterSet);
        sb.append(", statusFlags=").append(statusFlags);
        sb.append(", authPluginDataLen=").append(authPluginDataLen);
        sb.append(", authPluginDataPartTwo='").append(authPluginDataPartTwo).append('\'');
        sb.append(", authPluginName='").append(authPluginName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
