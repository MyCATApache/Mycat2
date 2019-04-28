package io.mycat.proxy.packet;

public class LongDataPacketImpl implements LongDataPacket {
    long statementId;
    int paramId;
    byte[] data;

    @Override
    public void setLongDataStatementId(long statementId) {
        this.statementId = statementId;
    }

    @Override
    public void setLongDataParamId(int paramId) {
        this.paramId = paramId;
    }

    @Override
    public void setLongData(byte[] longData) {
        this.data = longData;
    }

    @Override
    public long getLongDataStatementId() {
        return statementId;
    }

    @Override
    public long getLongDataParamId() {
        return paramId;
    }

    @Override
    public byte[] getLongData() {
        return data;
    }
}
