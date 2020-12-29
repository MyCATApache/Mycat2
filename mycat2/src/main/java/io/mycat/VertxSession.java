package io.mycat;

import io.mycat.proxy.session.MySQLServerSession;
import io.vertx.core.net.NetSocket;

public interface VertxSession extends MySQLServerSession {

    MycatDataContext getDataContext();

    void close();

    NetSocket getSocket();
}
