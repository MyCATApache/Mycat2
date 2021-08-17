/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.commands;

import io.mycat.MetaClusterCurrent;
import io.mycat.NativeMycatServer;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.MycatRouterConfig;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.replica.InstanceType;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
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
                        MycatDatasourcePool nativeDatasourcePool = createNativeDatasourcePool(name);
                        futureList.add(nativeDatasourcePool.getConnection()
                                .flatMap(c -> c.close().map(nativeDatasourcePool))
                                .recover(throwable -> Future.succeededFuture(createJdbcDatasourcePool(name))));
                        break;
                    }
                case JDBC:
                    hashMap.put(name, createJdbcDatasourcePool(name));
                    break;

            }
        }
        CompositeFuture.join((List) futureList).toCompletionStage().toCompletableFuture().get(1, TimeUnit.MINUTES);
        for (Future<MycatDatasourcePool> future : futureList) {
            MycatDatasourcePool datasourcePool = future.result();
            hashMap.put(datasourcePool.getTargetName(), datasourcePool);
        }
        this.map = hashMap;

    }

    @NotNull
    private MycatDatasourcePool createNativeDatasourcePool(String name) {
        NativeDatasourcePoolImpl nativeDatasourcePool = new NativeDatasourcePoolImpl(name);
        return new MonitorMycatDatasourcePool(nativeDatasourcePool);
    }

    @NotNull
    private MycatDatasourcePool createJdbcDatasourcePool(String name) {
        JdbcDatasourcePoolImpl jdbcDatasourcePool = new JdbcDatasourcePoolImpl(name);
        return new MonitorMycatDatasourcePool(jdbcDatasourcePool);
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
    @SneakyThrows
    public Map<String, java.sql.Connection> getWriteableConnectionMap() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        Map<String, JdbcDataSource> datasourceInfo = jdbcConnectionManager.getDatasourceInfo();
        HashMap<String, Connection> map = new HashMap<>();
        for (String string : datasourceInfo.keySet()) {
            ///////////////////////////////////////
            JdbcDataSource jdbcDataSource = datasourceInfo.get(string);
            DatasourceConfig config = jdbcDataSource.getConfig();
            if (jdbcDataSource.isMySQLType()) {
                if (Optional.ofNullable(config.getInstanceType()).map(i -> InstanceType.valueOf(i.toUpperCase()))
                        .orElse(InstanceType.READ_WRITE)
                        .isWriteType()) {
                    Connection connection = jdbcDataSource.getDataSource().getConnection();
                    map.put(string, connection);
                }
            }
        }
        return map;
    }

    @Override
    public Future<Void> close() {
        return Future.succeededFuture();
    }

    @Override
    public Future<Map<String, Integer>> computeConnectionUsageSnapshot() {
        HashMap<String, Integer> resMap = new HashMap<>();
        for (Map.Entry<String, MycatDatasourcePool> entry : map.entrySet()) {
            MycatDatasourcePool pool = entry.getValue();
            Integer n = pool.getAvailableNumber();
            resMap.put(entry.getKey(), n);
        }
        return Future.succeededFuture(resMap);
    }
}

