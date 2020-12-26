package io.mycat;

import io.mycat.proxy.session.MySQLServerSession;

public interface VertxSession extends MySQLServerSession {

    MycatDataContext getDataContext();

    void close();
}
