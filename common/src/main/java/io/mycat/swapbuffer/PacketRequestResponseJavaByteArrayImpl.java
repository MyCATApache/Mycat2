package io.mycat.swapbuffer;

import io.vertx.core.buffer.Buffer;

import java.nio.ByteBuffer;

public class PacketRequestResponseJavaByteArrayImpl extends PacketRequestResponse {
    final byte[] bytes;

    public PacketRequestResponseJavaByteArrayImpl(byte[] bytes) {
        this.bytes = bytes;
    }

    public static PacketRequestResponse wrap(byte[] bytes) {
        return new PacketRequestResponseJavaByteArrayImpl(bytes);
    }

    @Override
    public byte[] asJavaByteArray() {
        return this.bytes;
    }

    @Override
    public Buffer asJavaVertxBuffer() {
        return Buffer.buffer(bytes);
    }

    @Override
    public ByteBuffer asJavaByteBuffer() {
        return ByteBuffer.wrap(bytes);
    }
}
