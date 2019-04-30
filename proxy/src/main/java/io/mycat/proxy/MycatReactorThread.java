package io.mycat.proxy;

import io.mycat.proxy.packet.PacketSplitterImpl;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.proxy.session.FrontSessionManager;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.router.ByteArrayView;
import io.mycat.router.RouteResult;

import java.io.IOException;
import java.util.function.Consumer;

public final class MycatReactorThread extends ProxyReactorThread<MycatSession> {
    final MySQLSessionManager mySQLSessionManager = new MySQLSessionManager();
    final PacketSplitterImpl packetSplitter = new PacketSplitterImpl();

    public PacketSplitterImpl getPacketSplitter() {
        return packetSplitter;
    }
    public MycatReactorThread(BufferPool bufPool, FrontSessionManager<MycatSession> sessionManager) throws IOException {
        super(bufPool, sessionManager);
    }
    public MySQLSessionManager getMySQLSessionManager() {
        return mySQLSessionManager;
    }

}
