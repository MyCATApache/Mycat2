package io.mycat.swapbuffer;

import java.nio.ByteBuffer;

public class FixSwapBufferSenderImpl extends SwapBufferSender {
    public FixSwapBufferSenderImpl(ByteBuffer buffer) {
        super(buffer);
    }

    @Override
    public void subscribe(SwapBufferSender observer) {
        System.out.println();
    }
}
