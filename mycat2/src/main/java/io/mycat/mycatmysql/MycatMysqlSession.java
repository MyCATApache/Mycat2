package io.mycat.mycatmysql;

import cn.mycat.vertx.xa.XaSqlConnection;
import cn.mycat.vertx.xa.impl.BaseXaSqlConnection;
import io.mycat.runtime.MycatDataContextImpl;
import io.mycat.vertx.VertxSessionImpl;
import io.vertx.core.net.NetSocket;

public class MycatMysqlSession extends VertxSessionImpl {

    private final XaSqlConnection connection;

    public MycatMysqlSession(MycatDataContextImpl mycatDataContext,
                             NetSocket socket,
                             XaSqlConnection connection) {
        super(mycatDataContext, socket);
        this.connection = connection;
    }

    public XaSqlConnection getXaConnection() {
        return connection;
    }
}
