package io.mycat.buffer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class HeapBufferPool  implements BufferPool  {

    private int chunkSize;
    private int pageSize;
    private int pageCount;

    @Override
    public void init(Map<String, String> args) {
        String chunkSizeText = args.get("chunkSize");
        String pageSizeText = args.get("pageSize");
        String pageCountText = args.get("pageCount");

        this.chunkSize = Integer.parseInt(chunkSizeText);
        this.pageSize = Integer.parseInt(pageSizeText);
        this.pageCount = Integer.parseInt(pageCountText);
    }

    @Override
    public ByteBuffer allocate() {
        return ByteBuffer.allocate(chunkSize);
    }

    @Override
    public ByteBuffer allocate(int size) {
        return ByteBuffer.allocate(size);
    }

    @Override
    public ByteBuffer allocate(byte[] bytes) {
        return ByteBuffer.wrap(Arrays.copyOf(bytes,bytes.length));
    }

    @Override
    public void recycle(ByteBuffer theBuf) {

    }

    @Override
    public long capacity() {
        return chunkSize;
    }

    @Override
    public int chunkSize() {
        return chunkSize;
    }
}