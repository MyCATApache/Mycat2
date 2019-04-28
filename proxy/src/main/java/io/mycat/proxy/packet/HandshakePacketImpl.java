package io.mycat.proxy.packet;


import io.mycat.proxy.MycatExpection;
import io.mycat.beans.mysql.MySQLCapabilityFlags;
import io.mycat.proxy.payload.MySQLPayload;
import io.mycat.proxy.payload.MySQLPayloadReadView;
import io.mycat.proxy.payload.MySQLPayloadReader;

/**
 * cjw
 * 294712221@qq.com
 */
public class HandshakePacketImpl {
    public int protocolVersion;
    public String serverVersion;
    public long connectionId;
    public String authPluginDataPartOne;//salt auth plugin sql part 1
    public MySQLCapabilityFlags capabilities;
    public boolean hasPartTwo = false;
    public int characterSet;
    public int statusFlags;
    public int authPluginDataLen;
    public String authPluginDataPartTwo;
    public String authPluginName;

    public void readPayload(MySQLPayloadReader buffer) {
        protocolVersion = buffer.readByte();
        if (protocolVersion != 0x0a) {
            throw new MycatExpection("unsupport HandshakeV9");
        }
        serverVersion = buffer.readNULString();
        connectionId = buffer.readFixInt(4);
        authPluginDataPartOne = buffer.readFixString(8);
        buffer.skipInReading(1);
        capabilities = new MySQLCapabilityFlags((int) buffer.readFixInt(2) & 0x0000ffff);
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

    public void writePayload(MySQLPacket buffer) {
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
//        buffers.writeFixInt(2, this.capabilities.getLower2Bytes());
        size += 8 + 1 + 2;
        if (hasPartTwo) {
//            buffers.writeByte((byte) characterSet);
//            buffers.writeFixInt(2, this.statusFlags);
//            buffers.writeFixInt(2, this.capabilities.getUpper2Bytes());
            size += 1 + 2 + 2;
//            if (this.capabilities.isPluginAuth()) {
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
