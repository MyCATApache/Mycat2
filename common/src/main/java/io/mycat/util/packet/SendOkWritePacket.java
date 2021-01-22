package io.mycat.util.packet;

public abstract class SendOkWritePacket extends AbstractSocketWritePacket {
    @Override
    public Class<? extends AbstractSocketWritePacket> javaClass() {
        return SendOkWritePacket.class;
    }
}
