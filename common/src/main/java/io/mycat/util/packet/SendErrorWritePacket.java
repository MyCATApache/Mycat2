package io.mycat.util.packet;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public abstract class SendErrorWritePacket extends AbstractSocketWritePacket {
    private String errorMessage;
    private Throwable error;
    private int errorCode;
    @Override
    public Class<? extends AbstractSocketWritePacket> javaClass() {
        return SendErrorWritePacket.class;
    }
}
