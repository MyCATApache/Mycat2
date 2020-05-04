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


import io.mycat.ExecutorUtil;
import io.mycat.MycatConfig;
import io.mycat.MycatException;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.config.ClusterRootConfig;
import io.mycat.config.DatasourceRootConfig;
import io.mycat.config.ServerConfig;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasourceProvider.AtomikosDatasourceProvider;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.PlugRuntime;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.replica.heartbeat.HeartBeatStrategy;
import io.mycat.util.nio.SelectorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

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
    private  ExecutorService executorService;


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
        ServerConfig.Worker worker = config.getServer().getWorker();
        int maxThread = worker.getMaxThread();
        executorService  = ExecutorUtil.create("heartBeatExecutor", 1);

        if (!config.getServer().getWorker().isClose()) {
            PlugRuntime.INSTCANE.load(config);
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
                if (datasource.isJdbcType()) {
                    addDatasource(datasource);
                }
            }

            for (ClusterRootConfig.ClusterConfig replica : config.getCluster().getClusters()) {
                if ("jdbc".equals(replica.getHeartbeat().getReuqestType())) {
                    String replicaName = replica.getName();
                    for (String datasource : replica.getAllDatasources()) {
                        putHeartFlow(replicaName, datasource);
                    }
                }
            }

        }
    }


    private void putHeartFlow(String replicaName, String datasource) {

        ReplicaSelectorRuntime.INSTANCE.putHeartFlow(replicaName, datasource, new Consumer<HeartBeatStrategy>() {
            @Override
            public void accept(HeartBeatStrategy heartBeatStrategy) {
                executorService.submit(() -> {
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

    public int getMaxThread() {
        return config.getServer().getWorker().getMaxThread();
    }

    public int getWaitTaskTimeout() {
        return config.getServer().getWorker().getWaitTaskTimeout();
    }

    public String getTimeUnit() {
        return config.getServer().getWorker().getTimeUnit();
    }

    public int getMaxPengdingLimit() {
        return config.getServer().getWorker().getMaxPengdingLimit();
    }

    public JdbcConnectionManager getConnectionManager() {
        if (connectionManager != null ){
            return connectionManager;
        }else {
            throw new MycatException("jdbc连接管理器没有初始化,请配置jdbc连接");
        }

    }}