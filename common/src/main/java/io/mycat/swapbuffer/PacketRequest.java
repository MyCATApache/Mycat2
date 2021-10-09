package io.mycat.swapbuffer;

import io.vertx.core.buffer.Buffer;

import java.nio.ByteBuffer;

//供写入的数据区域
public interface PacketRequest {


    public static final PacketRequest END = new PacketRequest() {

        @Override
        public PacketResponse response(int copyCount) {
            return null;
        }

        @Override
        public byte[] asJavaByteArray() {
            return new byte[0];
        }

        @Override
        public Buffer asJavaVertxBuffer() {
            return null;
        }

        @Override
        public ByteBuffer asJavaByteBuffer() {
            return null;
        }

        @Override
        public int length() {
            return 0;
        }

        @Override
        public int offset() {
            return 0;
        }
    };

    public PacketResponse response(int copyCount);

    public default PacketResponse response() {
        return response(length());
    }

    public byte[] asJavaByteArray();

    public Buffer asJavaVertxBuffer();

    public ByteBuffer asJavaByteBuffer();

    public int length();

    int offset();
}