package io.mycat.util.packet;

public abstract class SendResultSetWritePacket extends AbstractSocketWritePacket {
    @Override
    public Class<? extends AbstractSocketWritePacket> javaClass() {
        return SendResultSetWritePacket.class;
    }
}
