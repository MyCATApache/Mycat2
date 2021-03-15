package io.mycat.mycatmysql;

import io.mycat.MycatDataContext;
import io.mycat.runtime.MycatDataContextImpl;
import io.mycat.vertx.VertxSessionImpl;
import io.vertx.core.net.NetSocket;

public class MycatMysqlSession extends VertxSessionImpl {


    public MycatMysqlSession(MycatDataContext mycatDataContext,
                             NetSocket socket) {
        super(mycatDataContext, socket);
    }
}
