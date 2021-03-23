package io.mycat.mycatmysql;

import io.mycat.MycatDataContext;
import io.mycat.runtime.MycatDataContextImpl;
import io.mycat.vertx.VertxSessionImpl;
import io.vertx.core.net.NetSocket;

public class MycatVertxMysqlSession extends VertxSessionImpl {


    public MycatVertxMysqlSession(MycatDataContext mycatDataContext,
                                  NetSocket socket) {
        super(mycatDataContext, socket);
    }
}
