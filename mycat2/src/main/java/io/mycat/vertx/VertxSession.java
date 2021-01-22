package io.mycat.vertx;

import io.mycat.MycatDataContext;
import io.mycat.proxy.session.MySQLServerSession;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.core.net.NetSocket;

public interface VertxSession extends MySQLServerSession {

    MycatDataContext getDataContext();

    PromiseInternal<Void> close();

    NetSocket getSocket();
}
