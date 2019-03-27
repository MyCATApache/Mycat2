package io.mycat.mysql;

import io.mycat.mysql.packet.PacketSplitter;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.buffer.BufferPool;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

public class MultiPacketWriter implements Iterator<ProxyBuffer> {
    private List<ProxyBuffer> byteBuffers;
    int index = 0;

    public MultiPacketWriter() {
    }

    public void init(List<ProxyBuffer> byteBuffers) {
        this.byteBuffers = byteBuffers;
        this.index = 0;
    }


    public boolean hasNext() {
        return byteBuffers.size()>index;
    }

    public void clear(){
        byteBuffers.clear();
        index = 0;
    }

    public ProxyBuffer next() {
        int index = this.index;
        this.index++;
        ProxyBuffer remove = byteBuffers.get(index);
        remove.readIndex = remove.writeIndex;
        remove.flip();
        return remove;
    }
}
