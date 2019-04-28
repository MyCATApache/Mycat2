package io.mycat.proxy.task;

import java.nio.MappedByteBuffer;

public class MappedByteBufferPayloadWriter extends AbstractByteBufferPayloadWriter<MappedByteBuffer> {

    @Override
    void clearResource(MappedByteBuffer f) throws Exception {
        f.force();
    }

}
