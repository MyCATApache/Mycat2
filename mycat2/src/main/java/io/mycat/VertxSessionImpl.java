package io.mycat;

import io.mycat.runtime.MycatDataContextImpl;
import io.vertx.core.net.NetSocket;

public class VertxSessionImpl implements VertxSession {
    private NetSocket socket;

    public VertxSessionImpl(NetSocket socket) {

        this.socket = socket;
    }

    @Override
    public int getCapabilities() {
        return 0;
    }

    @Override
    public MycatDataContext getDataContext() {
        return new MycatDataContextImpl();
    }

    @Override
    public void close() {

    }

    @Override
    public void sendOk() {

    }

    @Override
    public void writeError(Throwable throwable) {

    }
}
