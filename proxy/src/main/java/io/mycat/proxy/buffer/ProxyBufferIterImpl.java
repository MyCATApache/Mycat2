package io.mycat.proxy.buffer;

public class ProxyBufferIterImpl implements ProxyBufferWriteIter {
    ProxyBuffer proxyBuffer;
    @Override
    public boolean hasNext() {
        return proxyBuffer!=null;
    }

    @Override
    public boolean write(ProxyBuffer buffer) {
        return false;
    }

    @Override
    public void writeFinished(ProxyBuffer buffer) {

    }
}
