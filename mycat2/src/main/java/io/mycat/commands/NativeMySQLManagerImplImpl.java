package io.mycat.commands;

import io.mycat.MetaClusterCurrent;
import io.mycat.MycatWorkerProcessor;
import io.mycat.NameableExecutor;
import io.mycat.NativeMycatServer;
import io.mycat.proxy.MySQLDatasourcePool;
import io.mycat.util.VertxUtil;
import io.mycat.vertxmycat.AbstractMySqlConnectionImpl;
import io.vertx.core.Future;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.sqlclient.SqlConnection;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NativeMySQLManagerImplImpl extends AbstractMySQLManagerImpl {


    public Future<SqlConnection> getConnection(String targetName) {
        PromiseInternal<SqlConnection> promise = VertxUtil.newPromise();
        NativeMycatServer nativeMycatServer = MetaClusterCurrent.wrapper(NativeMycatServer.class);
        MySQLDatasourcePool sqlDatasourcePool = nativeMycatServer.getDatasource(targetName);
        sqlDatasourcePool.createSession().onComplete(event -> {
            MycatWorkerProcessor workerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
            NameableExecutor mycatWorker = workerProcessor.getMycatWorker();
            if (event.failed()) {
                mycatWorker.execute(() -> promise.tryFail(event.cause()));
            } else {
                mycatWorker.execute(() -> promise.tryComplete(new AbstractMySqlConnectionImpl(event.result())));
            }
        });
        return promise.future();
    }

    @Override
    public Future<Map<String, SqlConnection>> getConnectionMap() {
        NativeMycatServer nativeMycatServer = MetaClusterCurrent.wrapper(NativeMycatServer.class);
        return getMapFuture(nativeMycatServer.getDatasourceMap().keySet());
    }

    @Override
    public Future<Void> close() {
        return Future.succeededFuture();
    }

    @Override
    public Map<String, Integer> computeConnectionUsageSnapshot() {
        NativeMycatServer nativeMycatServer = MetaClusterCurrent.wrapper(NativeMycatServer.class);
        HashMap<String, Integer> datasourceInfo = new HashMap<>();
        ConcurrentHashMap<String, MySQLDatasourcePool> datasourceMap = nativeMycatServer.getDatasourceMap();
        for (Map.Entry<String, MySQLDatasourcePool> entry : datasourceMap.entrySet()) {
            String key = entry.getKey();
            MySQLDatasourcePool value = entry.getValue();
            datasourceInfo.put(key, value.getSessionLimitCount()-value.currentSessionCount());
        }
        return datasourceInfo;
    }
}
