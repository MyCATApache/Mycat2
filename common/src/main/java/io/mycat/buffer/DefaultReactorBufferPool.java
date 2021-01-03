package io.mycat.buffer;

import io.mycat.util.Dumper;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class DefaultReactorBufferPool extends HeapBufferPool implements ReactorBufferPool {

    private final Map<String, Object> sessionConfig;
    private final MycatDirectByteBufferPool directByteBufferPool;

    public DefaultReactorBufferPool(Map<String, Object> map) {
        this.sessionConfig = Objects.requireNonNull(map);
        int pageSize = Integer.parseInt(
                Objects.requireNonNull(sessionConfig.get("pageSize"), "pageSize must not be null").toString());
        short chunkSize = Short.parseShort(
                Objects.requireNonNull(sessionConfig.get("chunkSize"), "chunkSize must not be null").toString());

        short pageCount = Short.parseShort(
                Objects.requireNonNull(sessionConfig.get("pageCount"), "pageCount must not be null").toString());
        setChunkSize(chunkSize);

        this.directByteBufferPool =
                new MycatDirectByteBufferPool(pageSize, chunkSize, pageCount);
    }

    @Override
    public BufferPool newSessionBufferPool() {
        return new BufferPool() {
            @Override
            public ByteBuffer allocate() {
                return directByteBufferPool.allocate();
            }

            @Override
            public ByteBuffer allocate(int size) {
                return directByteBufferPool.allocate(size);
            }

            @Override
            public ByteBuffer allocate(byte[] bytes) {
                return directByteBufferPool.allocate(bytes);
            }

            @Override
            public int trace() {
                return (int) directByteBufferPool.usage();
            }

            @Override
            public void recycle(ByteBuffer theBuf) {
                directByteBufferPool.recycle(theBuf);
            }

            @Override
            public long capacity() {
                return directByteBufferPool.capacity();
            }

            @Override
            public int chunkSize() {
                return directByteBufferPool.getChunkSize();
            }

            @Override
            public Dumper snapshot() {
                return Dumper.create(Collections.singletonMap("trace", trace()));
            }
        };
    }
}
