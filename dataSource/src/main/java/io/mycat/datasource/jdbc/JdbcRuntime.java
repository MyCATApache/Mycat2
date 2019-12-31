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
import io.mycat.MycatException;
import io.mycat.bindThread.BindThread;
import io.mycat.bindThread.BindThreadCallback;
import io.mycat.bindThread.BindThreadKey;
import io.mycat.config.ClusterRootConfig;
import io.mycat.config.DatasourceRootConfig;
import io.mycat.datasource.jdbc.datasource.*;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
import io.mycat.datasource.jdbc.thread.GThread;
import io.mycat.datasource.jdbc.thread.GThreadPool;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.PlugRuntime;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.replica.heartbeat.HeartBeatStrategy;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author Junwen Chen
 **/
public enum JdbcRuntime {
    INSTANCE;
    private final MycatLogger LOGGER = MycatLoggerFactory.getLogger(JdbcRuntime.class);
    private GThreadPool gThreadPool;
    private JdbcConnectionManager connectionManager;
    private MycatConfig config;
    private DatasourceProvider datasourceProvider;

    public void addDatasource(DatasourceRootConfig.DatasourceConfig key) {
        connectionManager.addDatasource(key);
    }

    public void removeDatasource(String jdbcDataSourceName) {
        connectionManager.removeDatasource(jdbcDataSourceName);
    }

    public DefaultConnection getConnection(String name) {
        return connectionManager.getConnection(name);
    }

    public DefaultConnection getConnection(String name, boolean autocommit, int transactionIsolation) {
        return connectionManager.getConnection(name, autocommit, transactionIsolation);
    }

    public void closeConnection(DefaultConnection connection) {
        connectionManager.closeConnection(connection);
    }

    public boolean isJTA() {
        return connectionManager.isJTA();
    }

    public void load(MycatConfig config) {
        PlugRuntime.INSTCANE.load(config);
        ReplicaSelectorRuntime.INSTANCE.load(config);
        this.config = config;
        try {
            this.datasourceProvider = (DatasourceProvider) Class.forName(config.getDatasource().getDatasourceProviderClass())
                    .getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new MycatException("can not load datasourceProvider:{}", config.getDatasource().getDatasourceProviderClass());
        }
        connectionManager = new JdbcConnectionManager(this.datasourceProvider);
        gThreadPool = new GThreadPool(this);

        for (ClusterRootConfig.ClusterConfig replica : config.getReplicas().getReplicas()) {
            if ("jdbc".equals(replica.getHeartbeat().getReuqestType())) {
                String replicaName = replica.getName();
                for (String datasource : replica.getMasters()) {
                    putHeartFlow(replicaName, datasource);
                }
            }
        }
    }

    private void putHeartFlow(String replicaName, String datasource) {
        ReplicaSelectorRuntime.INSTANCE.putHeartFlow(replicaName, datasource, new Consumer<HeartBeatStrategy>() {
            @Override
            public void accept(HeartBeatStrategy heartBeatStrategy) {
                gThreadPool.run(() -> false, new BindThreadCallback() {
                    @Override
                    public void accept(BindThreadKey key, BindThread context) {
                        heartbeat(heartBeatStrategy);
                    }

                    @Override
                    public void onException(BindThreadKey key, Exception e) {
                        heartBeatStrategy.onException(e);
                    }
                });

            }

            private void heartbeat(HeartBeatStrategy heartBeatStrategy) {
                DefaultConnection connection = null;
                try {
                    connection  = getConnection(datasource);
                        List<Map<String, Object>> resultList;
                        try (JdbcRowBaseIteratorImpl iterator = connection
                                .executeQuery(heartBeatStrategy.getSql())) {
                            resultList = iterator.getResultSetMap();
                        }
                        heartBeatStrategy.process(resultList);
                    } catch (Exception e) {
                        heartBeatStrategy.onException(e);
                        throw e;
                    } finally {
                        if (connection != null) {
                            connection.close();
                        }
                    }
            }
        });
    }


    public TransactionSession createTransactionSession(GThread gThread) {
        if (isJTA()) {
            return new JTATransactionSessionImpl(datasourceProvider.createUserTransaction(), gThread);
        } else {
            return new LocalTransactionSessionImpl(gThread);
        }
    }

    public DatasourceProvider getDatasourceProvider() {
        return datasourceProvider;
    }

    public <K extends BindThreadKey, T extends BindThreadCallback> boolean run(K key, T processTask) {
        return gThreadPool.run(key, processTask);
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
}