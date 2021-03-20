package io.mycat.commands;

import io.mycat.MetaClusterCurrent;
import io.mycat.NativeMycatServer;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.MycatRouterConfig;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;
import lombok.SneakyThrows;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class MycatMySQLManagerImpl extends AbstractMySQLManagerImpl {

    private final ConcurrentHashMap<String, MycatDatasourcePool> map;

    @SneakyThrows
    public MycatMySQLManagerImpl(MycatRouterConfig config) {
        boolean nativeServer = MetaClusterCurrent.exist(NativeMycatServer.class);
        ConcurrentHashMap<String, MycatDatasourcePool> hashMap = new ConcurrentHashMap<>();
        MycatRouterConfig mycatRouterConfig = config;
        List<Future<MycatDatasourcePool>> futureList = new ArrayList<>();
        for (DatasourceConfig datasource : mycatRouterConfig.getDatasources()) {
            String name = datasource.getName();
            switch (datasource.computeType()) {
                case NATIVE:
                case NATIVE_JDBC:
                    if (nativeServer) {
                        NativeDatasourcePoolImpl nativeDatasourcePool = new NativeDatasourcePoolImpl(name);
                        futureList.add(nativeDatasourcePool.getConnection()
                                .flatMap(c -> c.close().map((MycatDatasourcePool) nativeDatasourcePool))
                                .recover(throwable -> Future.succeededFuture(new JdbcDatasourcePoolImpl(name))));
                        break;
                    }
                case JDBC:
                    hashMap.put(name, new JdbcDatasourcePoolImpl(name));
                    break;

            }
        }
        CompositeFuture.all((List) futureList).toCompletionStage().toCompletableFuture().get(1, TimeUnit.MINUTES);
        for (Future<MycatDatasourcePool> future : futureList) {
            MycatDatasourcePool datasourcePool = future.result();
            hashMap.put(datasourcePool.getTargetName(), datasourcePool);
        }
        this.map = hashMap;

    }

    @Override
    public Future<SqlConnection> getConnection(String targetName) {
        MycatDatasourcePool mycatDatasourcePool = Objects.requireNonNull(map.get(targetName));
        return mycatDatasourcePool.getConnection();
    }

    @Override
    public int getSessionCount(String targetName) {
        return map.get(targetName).getUsedNumber();
    }

    @Override
    public Future<Map<String, SqlConnection>> getConnectionMap() {
        return getMapFuture(map.keySet());
    }

    @Override
    public Future<Void> close() {
        return Future.succeededFuture();
    }

    @Override
    public Future<Map<String, Integer>> computeConnectionUsageSnapshot() {
        HashMap<String,Integer> resMap = new HashMap<>();
        for (Map.Entry<String, MycatDatasourcePool> entry : map.entrySet()) {
            MycatDatasourcePool pool = entry.getValue();
            Integer n = pool.getAvailableNumber();
            resMap.put(entry.getKey(),n);
        }
        return Future.succeededFuture(resMap);
    }
}

