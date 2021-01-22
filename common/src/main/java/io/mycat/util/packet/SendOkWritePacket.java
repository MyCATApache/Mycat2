package io.mycat.util.packet;

public abstract class SendOkWritePacket extends AbstractWritePacket {
    @Override
    public Class<? extends AbstractWritePacket> javaClass() {
        return SendOkWritePacket.class;
    }
}
