/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.datasource.jdbc;


import io.mycat.MycatConfig;
import io.mycat.MycatConnection;
import io.mycat.MycatException;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.config.ClusterRootConfig;
import io.mycat.config.DatasourceRootConfig;
import io.mycat.config.ServerConfig;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.datasource.jdbc.datasourceprovider.AtomikosDatasourceProvider;
import io.mycat.plug.PlugRuntime;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.replica.heartbeat.HeartBeatStrategy;
import io.mycat.MycatWorkerProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;

/**
 * @author Junwen Chen
 **/
public enum JdbcRuntime {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcRuntime.class);
    private JdbcConnectionManager connectionManager;
    private MycatConfig config;
    private DatasourceProvider datasourceProvider;

    public void addDatasource(DatasourceRootConfig.DatasourceConfig key) {
        connectionManager.addDatasource(key);
    }

    public void removeDatasource(String jdbcDataSourceName) {
        connectionManager.removeDatasource(jdbcDataSourceName);
    }

    public DefaultConnection getConnection(String name, Boolean autocommit, int transactionIsolation, boolean readOnly) {
        return connectionManager.getConnection(name, autocommit, transactionIsolation, readOnly);
    }

    public DefaultConnection getConnection(String name) {
        return connectionManager.getConnection(name, true, TRANSACTION_REPEATABLE_READ, false);
    }

    public void closeConnection(DefaultConnection connection) {
        connectionManager.closeConnection(connection);
    }

    public synchronized void load(MycatConfig config) {
        ServerConfig.ThreadPoolExecutorConfig worker = config.getServer().getWorkerPool();
        MycatWorkerProcessor.INSTANCE.init(worker, config.getServer().getTimeWorkerPool());
        PlugRuntime.INSTANCE.load(config);
        ReplicaSelectorRuntime.INSTANCE.load(config);
        this.config = config;
        String customerDatasourceProvider = config.getDatasource().getDatasourceProviderClass();
        String defaultDatasourceProvider = Optional.ofNullable(customerDatasourceProvider).orElse(AtomikosDatasourceProvider.class.getName());
        try {
            this.datasourceProvider = (DatasourceProvider) Class.forName(defaultDatasourceProvider)
                    .getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new MycatException("can not load datasourceProvider:{}", config.getDatasource().getDatasourceProviderClass());
        }
        connectionManager = new JdbcConnectionManager(this.datasourceProvider);


        for (DatasourceRootConfig.DatasourceConfig datasource : config.getDatasource().getDatasources()) {
            if (datasource.computeType().isJdbc()) {
                addDatasource(datasource);
            }
        }

        for (ClusterRootConfig.ClusterConfig replica : config.getCluster().getClusters()) {
            if ("jdbc".equalsIgnoreCase(replica.getHeartbeat().getRequestType())) {
                String replicaName = replica.getName();
                for (String datasource : replica.getAllDatasources()) {
                    putHeartFlow(replicaName, datasource);
                }
            }
        }

        //移除不必要的配置
        //新配置中的数据源名字
        Set<String> datasourceNames = config.getDatasource().getDatasources().stream().map(i -> i.getName()).collect(Collectors.toSet());
        Map<String, JdbcDataSource> datasourceInfo = connectionManager.getDatasourceInfo();
        new HashSet<>(datasourceInfo.keySet()).stream().filter(name->!datasourceNames.contains(name)).forEach(name->connectionManager.removeDatasource(name));

    }


    private void putHeartFlow(String replicaName, String datasource) {

        ReplicaSelectorRuntime.INSTANCE.putHeartFlow(replicaName, datasource, new Consumer<HeartBeatStrategy>() {
            @Override
            public void accept(HeartBeatStrategy heartBeatStrategy) {
                MycatWorkerProcessor.INSTANCE.getMycatWorker().submit(() -> {
                    try {
                        heartbeat(heartBeatStrategy);
                    } catch (Exception e) {
                        heartBeatStrategy.onException(e);
                    }
                });
            }

            private void heartbeat(HeartBeatStrategy heartBeatStrategy) {
                DefaultConnection connection = null;
                try {
                    connection = getConnection(datasource);
                    List<Map<String, Object>> resultList;
                    try (RowBaseIterator iterator = connection
                            .executeQuery(heartBeatStrategy.getSql())) {
                        resultList = iterator.getResultSetMap();
                    }
                    LOGGER.debug("jdbc heartbeat {}", Objects.toString(resultList));
                    heartBeatStrategy.process(resultList);
                } catch (Exception e) {
                    heartBeatStrategy.onException(e);
                    throw e;
                } catch (Throwable e) {
                    LOGGER.error("", e);
                } finally {
                    if (connection != null) {
                        connection.close();
                    }
                }
            }
        });
    }

    public DatasourceProvider getDatasourceProvider() {
        return datasourceProvider;
    }


    public String getTimeUnit() {
        return config.getServer().getBindTransactionPool().getTimeUnit();
    }

    public int getMaxPengdingLimit() {
        return config.getServer().getBindTransactionPool().getMaxPendingLimit();
    }

    public JdbcConnectionManager getConnectionManager() {
        if (connectionManager != null) {
            return connectionManager;
        } else {
            throw new MycatException("jdbc连接管理器没有初始化,请配置jdbc连接");
        }

    }

    public ExecutorService getFetchDataExecutorService() {
        return MycatWorkerProcessor.INSTANCE.getMycatWorker();
    }
}