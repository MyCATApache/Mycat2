package io.mycat.commands;

import io.mycat.MetaClusterCurrent;
import io.mycat.NativeMycatServer;
import io.mycat.proxy.MySQLDatasourcePool;
import io.mycat.vertxmycat.AbstractMySqlConnectionImpl;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlConnection;

public class NativeDatasourcePoolImpl extends MycatDatasourcePool {
    public NativeDatasourcePoolImpl(String targetName) {
        super(targetName);
    }

    @Override
    public Future<SqlConnection> getConnection() {
        return Future.future(promise -> {
            NativeMycatServer nativeMycatServer = MetaClusterCurrent.wrapper(NativeMycatServer.class);
            MySQLDatasourcePool sqlDatasourcePool = nativeMycatServer.getDatasource(targetName);
            sqlDatasourcePool.createSession().flatMap(session -> {
                Vertx vertx = MetaClusterCurrent.wrapper(Vertx.class);
                return vertx
                        .executeBlocking((Handler<Promise<SqlConnection>>) event -> event.complete(new AbstractMySqlConnectionImpl(session)));
            }).onComplete(promise);
        });
    }

    @Override
    public Integer getAvailableNumber() {
        NativeMycatServer nativeMycatServer = MetaClusterCurrent.wrapper(NativeMycatServer.class);
        MySQLDatasourcePool sqlDatasourcePool = nativeMycatServer.getDatasource(targetName);
        return sqlDatasourcePool.getSessionLimitCount() - sqlDatasourcePool.currentSessionCount();
    }

    @Override
    public Integer getUsedNumber() {
        NativeMycatServer nativeMycatServer = MetaClusterCurrent.wrapper(NativeMycatServer.class);
        MySQLDatasourcePool sqlDatasourcePool = nativeMycatServer.getDatasource(targetName);
        return sqlDatasourcePool.currentSessionCount();
    }
}
