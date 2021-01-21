package io.mycat.vertx;

import io.mycat.MycatDataContext;
import io.mycat.proxy.session.MySQLServerSession;
import io.vertx.core.net.NetSocket;

public interface VertxSession extends MySQLServerSession {

    MycatDataContext getDataContext();

    void close();

    NetSocket getSocket();

    VertxMySQLPacketResolver getVertxMySQLPacketResolver();
}
