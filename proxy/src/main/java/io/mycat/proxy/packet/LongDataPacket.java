package io.mycat.proxy.packet;

public interface LongDataPacket {

    void setLongDataStatementId(long statementId);

    void setLongDataParamId(int paramId);

    void setLongData(byte[] longData);

    long getLongDataStatementId();

    long getLongDataParamId();

    byte[] getLongData();


    default void readPayload(MySQLPacket buffer, int payloadLength) {
        assert buffer.readByte() == 0x18;
        this.setLongDataStatementId(buffer.readFixInt(4));
        this.setLongDataParamId((int) buffer.readFixInt(2));
        this.setLongData(buffer.readBytes(payloadLength - 7));
    }


    default void writePayload(MySQLPacket buffer) {
        buffer.writeByte((byte) 0x18);
        buffer.writeFixInt(4, getLongDataStatementId());
        buffer.writeFixInt(2, getLongDataParamId());
        buffer.writeBytes(getLongData());
    }
}
