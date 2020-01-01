package io.mycat.proxy.session;

import io.mycat.ProxyBeanProviders;
import io.mycat.config.DatasourceRootConfig;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.handler.backend.MySQLDataSourceQuery;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.PhysicsInstanceImpl;
import io.mycat.replica.ReplicaSelectorRuntime;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MySQLReplicaSessionManager {
    protected static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(MySQLReplicaSessionManager.class);
    private final ProxyBeanProviders dataSourceFactory;
    private final MySQLSessionManager mySQLSessionManager;
    private final ConcurrentHashMap<String, MySQLDatasource> datasourceMap = new ConcurrentHashMap<>();

    public MySQLReplicaSessionManager(ProxyBeanProviders dataSourceFactory, MySQLSessionManager mySQLSessionManager) {
        this.dataSourceFactory = dataSourceFactory;
        this.mySQLSessionManager = mySQLSessionManager;
    }

    public synchronized void addDatasource(DatasourceRootConfig.DatasourceConfig datasource) {
        if (datasource.isMySQLType()) {
            datasourceMap.put(datasource.getName(), dataSourceFactory.createDatasource(datasource));
        } else {
            LOGGER.warn("ignore add datasource in mysql native{}", datasource.getName());
        }
    }

    public synchronized void removeDatasource(String datasourceName, String reason) {
        mySQLSessionManager.clearAndDestroyDataSource(datasourceMap.remove(datasourceName), reason);
    }

    public void getMySQLSessionByBalance(String replicaName, MySQLDataSourceQuery query,
                                         SessionCallBack<MySQLClientSession> asynTaskCallBack) {
        boolean isRunOnMaster = true;
        LoadBalanceStrategy lbs = null;
        List<SessionManager.SessionIdAble> ids = null;
        if (query != null) {
            isRunOnMaster = query.isRunOnMaster();
            lbs = query.getStrategy();
            ids = query.getIds();
        }
        MySQLDatasource datasource = getMySQLSessionByBalance(replicaName, isRunOnMaster, lbs);
        mySQLSessionManager.getSessionCallback(datasource, ids, this, asynTaskCallBack);
    }

    public MySQLDatasource getMySQLSessionByBalance(String replicaName, boolean runOnMaster,
                                                    LoadBalanceStrategy strategy) {
        PhysicsInstanceImpl instance =
                runOnMaster ? ReplicaSelectorRuntime.INSTANCE.getWriteDatasourceByReplicaName(replicaName, strategy)
                        : ReplicaSelectorRuntime.INSTANCE.getDatasourceByReplicaName(replicaName, strategy);
        return this.datasourceMap.get(instance.getName());
    }

}