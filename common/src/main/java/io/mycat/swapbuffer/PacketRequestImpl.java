package io.mycat.swapbuffer;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.nio.ByteBuffer;

@AllArgsConstructor
@Getter
public class PacketRequestImpl implements PacketRequest{
    final ByteBuffer body;
    final int offset;
    final int length;

    @Override
    public PacketResponse response(int copyCount) {
        return new PacketResponseImpl(this,copyCount);
    }

    public byte[] asJavaByteArray() {
        ByteBuffer byteBuffer = body.asReadOnlyBuffer();
        byteBuffer.position(offset);
        byte[] bytes = new byte[length];
        byteBuffer.get(bytes, offset, length);
        return bytes;
    }
    public Buffer asJavaVertxBuffer(){
       return Buffer.buffer(asJavaByteArray());
    }

    @Override
    public ByteBuffer asJavaByteBuffer() {
        ByteBuffer byteBuffer = body.asReadOnlyBuffer();
        byteBuffer.position(offset);
        byteBuffer.limit(offset+length);
        return byteBuffer;
    }

    @Override
    public int length() {
        return 0;
    }
}
