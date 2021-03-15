package io.mycat.util.packet;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public abstract class SendErrorWritePacket extends AbstractWritePacket {
    private String errorMessage;
    private Throwable error;
    private int errorCode;
    @Override
    public Class<? extends AbstractWritePacket> javaClass() {
        return SendErrorWritePacket.class;
    }
}
