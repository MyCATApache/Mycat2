package io.mycat.util.packet;

public abstract class SendResultSetWritePacket extends AbstractWritePacket {
    @Override
    public Class<? extends AbstractWritePacket> javaClass() {
        return SendResultSetWritePacket.class;
    }
}
