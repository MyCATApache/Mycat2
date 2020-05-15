package io.mycat;

import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaSelectorRuntime;

import java.util.Map;
import java.util.Objects;


/**
 * 整合jdbc与proxy数据源
 */
public class MycatDatasourceUtil {

    static final JdbcRuntime jdbcManager = JdbcRuntime.INSTANCE;
    static final MycatCore proxyManager = MycatCore.INSTANCE;

    public static PhysicsInstance getDataSourceInfo(String name) {
        ReplicaSelectorRuntime selectorRuntime = ReplicaSelectorRuntime.INSTANCE;
        PhysicsInstance physicsInstance = selectorRuntime.getPhysicsInstanceByName(name);
        Objects.requireNonNull(physicsInstance, "unknown Datasource:" + name);
        return physicsInstance;
    }

    public static boolean isJdbcDatasource(String name) {
        Map<String, JdbcDataSource> datasourceInfo = jdbcManager.getConnectionManager().getDatasourceInfo();
        return datasourceInfo.containsKey(name);
    }

    public static boolean isProxyDatasource(String name) {
        return proxyManager.getDatasource(name) != null;
    }

}