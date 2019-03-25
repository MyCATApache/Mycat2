package io.mycat.mysql.packet;

import io.mycat.proxy.ProxyBuffer;

/**
 * cjw
 * 294712221@qq.com
 * before COM_STMT_EXECUTE
 */
public class SendLongDataPacket extends MySQLPacket {
    private long statementId;
    private long paramId;
    private byte[] data = new byte[0];

    @Override
    public int calcPayloadSize() {
        return 1 + 4 + 2 + data.length;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Long Data Packet";
    }

    @Override
    public void write(ProxyBuffer buffer) {
        int pkgSize = calcPayloadSize();
        buffer.writeFixInt(3, pkgSize);
        buffer.writeByte(packetId);
        writePayload(buffer);
    }

    public void read(ProxyBuffer buffer) {
        packetLength = (int) buffer.readFixInt(3);
        packetId = buffer.readByte();
        readPayload(buffer);
    }

    private void readPayload(ProxyBuffer buffer) {
        this.statementId = buffer.readFixInt(4);
        this.paramId = buffer.readFixInt(2);
        this.data = buffer.readBytes(packetLength - 7);
    }


    private void writePayload(ProxyBuffer buffer) {
        buffer.writeByte((byte) 18);
        buffer.writeFixInt(4, statementId);
        buffer.writeFixInt(2, paramId);
        buffer.writeBytes(data);
    }
}
