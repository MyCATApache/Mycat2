package io.mycat.commands;

import io.mycat.MetaClusterCurrent;
import io.mycat.MycatWorkerProcessor;
import io.mycat.NameableExecutor;
import io.mycat.NativeMycatServer;
import io.mycat.proxy.MySQLDatasourcePool;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.util.VertxUtil;
import io.mycat.vertxmycat.AbstractMySqlConnectionImpl;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.future.PromiseInternal;
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
    public Future<Integer> getAvailableNumber() {
        return Future.future(promise -> {
            NativeMycatServer nativeMycatServer = MetaClusterCurrent.wrapper(NativeMycatServer.class);
            MySQLDatasourcePool sqlDatasourcePool = nativeMycatServer.getDatasource(targetName);
            promise.complete(sqlDatasourcePool.getSessionLimitCount() - sqlDatasourcePool.currentSessionCount());
        });
    }
}
