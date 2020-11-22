package io.mycat.buffer;

import io.mycat.util.Dumper;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class DefaultReactorBufferPool extends HeapBufferPool implements ReactorBufferPool {

    private final Map<String, Object> sessionConfig;

    public DefaultReactorBufferPool(Map<String, Object> map) {
        this.sessionConfig = Objects.requireNonNull(map);
        int pageSize = Integer.parseInt(
                Objects.requireNonNull(sessionConfig.get("pageSize"), "pageSize must not be null").toString());
        short chunkSize = Short.parseShort(
                Objects.requireNonNull(sessionConfig.get("chunkSize"), "chunkSize must not be null").toString());

        short pageCount = Short.parseShort(
                Objects.requireNonNull(sessionConfig.get("pageCount"), "pageCount must not be null").toString());
        setChunkSize(chunkSize);
    }

    @Override
    public BufferPool newSessionBufferPool() {
        HeapBufferPool heapBufferPool = new HeapBufferPool();
        heapBufferPool.setChunkSize(chunkSize());
        return heapBufferPool;
    }
}
