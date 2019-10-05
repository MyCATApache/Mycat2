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


import io.mycat.ConfigRuntime;
import io.mycat.MycatException;
import io.mycat.bindThread.BindThread;
import io.mycat.bindThread.BindThreadCallback;
import io.mycat.bindThread.BindThreadKey;
import io.mycat.config.ConfigFile;
import io.mycat.config.ConfigReceiver;
import io.mycat.config.ConfigurableRoot;
import io.mycat.config.datasource.*;
import io.mycat.config.heartbeat.HeartbeatConfig;
import io.mycat.config.heartbeat.HeartbeatRootConfig;
import io.mycat.config.plug.PlugRootConfig;
import io.mycat.config.schema.DataNodeConfig;
import io.mycat.config.schema.DataNodeRootConfig;
import io.mycat.datasource.jdbc.datasource.*;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
import io.mycat.datasource.jdbc.thread.GThread;
import io.mycat.datasource.jdbc.thread.GThreadPool;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.replica.ReplicaHeartbeatRuntime;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
/**
 * @author Junwen Chen
 **/
public enum GRuntime {
    INSTACNE;
    private final MycatLogger LOGGER = MycatLoggerFactory.getLogger(GRuntime.class);
    private final Map<String, JdbcReplica> jdbcReplicaMap = new HashMap<>();
    private final Map<String, JdbcDataNode> jdbcDataNodeMap = new HashMap<>();
    private final Map<String, JdbcDataSource> jdbcDataSourceMap = new HashMap<>();
    private final Map<String, Object> defContext = new HashMap<>();
    private GBeanProviders providers;
    private boolean isJTA;
    private DatasourceProvider datasourceProvider;
    private ConfigReceiver config;
    private GThreadPool gThreadPool;
    private ScheduledExecutorService schedule;
    private JdbcDriverRootConfig jdbcDriverRootConfig;

    GRuntime() {

    }

    public void load(ConfigReceiver config) {
        this.config = config;
        this.jdbcDriverRootConfig = this.getConfig(ConfigFile.JDBC_DRIVER);
        String gridBeanProvidersClass = "io.mycat.DefaultGridBeanProviders";
        try {
            this.providers = (GBeanProviders) Class.forName(gridBeanProvidersClass).newInstance();
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        ReplicasRootConfig dsConfig = config.getConfig(ConfigFile.DATASOURCE);
        MasterIndexesRootConfig replicaIndexConfig = config.getConfig(ConfigFile.REPLICA_INDEX);
        JdbcDriverRootConfig jdbcDriverRootConfig = config.getConfig(ConfigFile.JDBC_DRIVER);
        String datasourceProviderClass = jdbcDriverRootConfig.getDatasourceProviderClass();
        Objects.requireNonNull(datasourceProviderClass);
        PlugRootConfig plugRootConfig = config.getConfig(ConfigFile.PLUG);
        Objects.requireNonNull(plugRootConfig, "plug config can not found");
        try {
            this.datasourceProvider = (DatasourceProvider) Class.forName(datasourceProviderClass)
                    .newInstance();
        } catch (Exception e) {
            throw new MycatException("can not load datasourceProvider:{}", datasourceProviderClass);
        }
        isJTA = datasourceProvider.isJTA();
        initJdbcReplica(dsConfig, replicaIndexConfig,
                datasourceProvider);
        DataNodeRootConfig dataNodeRootConfig = config.getConfig(ConfigFile.DATANODE);
        initJdbcDataNode(dataNodeRootConfig);
        gThreadPool = new GThreadPool(this);

        initHeartbeat(dsConfig);
    }

    private void initHeartbeat(ReplicasRootConfig dsConfig) {
        HeartbeatRootConfig heartbeatRootConfig = getConfig(ConfigFile.HEARTBEAT);
        HeartbeatConfig heartbeatConfig = heartbeatRootConfig.getHeartbeat();
        boolean existUpdate = false;
        for (ReplicaConfig replica : dsConfig.getReplicas()) {
            List<DatasourceConfig> datasources = replica.getDatasources();
            if (datasources != null) {
                datasources = Collections.emptyList();
            }
            final BindThreadKey key = new BindThreadKey() {
            };
            for (DatasourceConfig datasource : Objects.requireNonNull(datasources)) {
                existUpdate = existUpdate || ReplicaHeartbeatRuntime.INSTANCE
                        .register(replica, datasource, heartbeatConfig,
                                heartBeatStrategy -> {
                                    run(key, new BindThreadCallback() {
                                        @Override
                                        public void accept(BindThreadKey key, BindThread context) {
                                            JdbcDataSource jdbcDataSource = jdbcDataSourceMap.get(datasource.getName());
                                            if (jdbcDataSource != null) {
                                                DsConnection connection = null;
                                                try {
                                                    connection = jdbcReplicaMap.get(replica.getName())
                                                            .getDefaultConnection(jdbcDataSource);
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
                                        }

                                        @Override
                                        public void onException(BindThreadKey key, Exception e) {
                                            heartBeatStrategy.onException(e);
                                        }
                                    });

                                });
            }
        }
        if (existUpdate) {
            long period = heartbeatConfig.getReplicaHeartbeatPeriod();
            if (schedule == null) {
                schedule = Executors.newScheduledThreadPool(1);
            }
            schedule.scheduleAtFixedRate(() -> {
                        try {
                            ReplicaHeartbeatRuntime.INSTANCE.heartbeat();
                        } catch (Exception e) {
                            LOGGER.error("", e);
                        }
                    }, 0, period,
                    TimeUnit.MILLISECONDS);
        }
    }

    public JdbcReplica getJdbcReplicaByReplicaName(String name) {
        JdbcReplica jdbcReplica = jdbcReplicaMap.get(name);
        Objects.requireNonNull(jdbcReplica);
        return jdbcReplica;
    }

    public JdbcDataSource getJdbcDatasourceSessionByReplicaName(String replicaName) {
        JdbcReplica replica = getJdbcReplicaByReplicaName(replicaName);
        return replica.getDataSourceByBalance(null);
    }

    public JdbcDataSource getJdbcDatasourceByName(String datasourceName) {
        Objects.requireNonNull(datasourceName);
        JdbcDataSource jdbcDataSource = jdbcDataSourceMap.get(datasourceName);
        return Objects.requireNonNull(jdbcDataSource);
    }

    public JdbcDataSource getJdbcDatasourceByDataNodeName(String dataNodeName,
                                                          JdbcDataSourceQuery query) {
        Objects.requireNonNull(dataNodeName);
        JdbcDataNode jdbcDataNode = jdbcDataNodeMap.get(dataNodeName);
        JdbcReplica replica = jdbcDataNode.getReplica();
        return replica.getDataSourceByBalance(query);
    }


    private void initJdbcReplica(ReplicasRootConfig replicasRootConfig,
                                 MasterIndexesRootConfig replicaIndexConfig,
                                 DatasourceProvider datasourceProvider) {
        if (replicasRootConfig != null && replicasRootConfig.getReplicas() != null
                && !replicasRootConfig.getReplicas().isEmpty()) {
            for (ReplicaConfig replicaConfig : replicasRootConfig.getReplicas()) {
                Set<Integer> replicaIndexes = ConfigRuntime.INSTCANE
                        .getReplicaIndexes(replicaConfig.getName());
                JdbcReplica jdbcReplica = providers.createJdbcReplica(this, replicaConfig,
                        replicaIndexes, replicaConfig.getDatasources(), datasourceProvider);
                jdbcReplicaMap.put(jdbcReplica.getName(), jdbcReplica);
                List<JdbcDataSource> datasourceList = jdbcReplica.getDatasourceList();
                for (JdbcDataSource jdbcDataSource : datasourceList) {
                    jdbcDataSourceMap.compute(jdbcDataSource.getName(),
                            (s, dataSource) -> {
                                if (dataSource != null) {
                                    throw new MycatException("duplicate name of jdbc datasource");
                                }
                                return jdbcDataSource;
                            });
                }
            }
        }
    }

    private void initJdbcDataNode(DataNodeRootConfig config) {
        if (config != null && config.getDataNodes() != null) {
            List<DataNodeConfig> dataNodes = config.getDataNodes();
            for (DataNodeConfig dataNode : dataNodes) {
                JdbcReplica jdbcReplica = jdbcReplicaMap.get(dataNode.getReplica());
                if (jdbcReplica == null) {
                    continue;
                }
                JdbcDataNode jdbcDataNode = new JdbcDataNode(jdbcReplica, dataNode);
                jdbcDataNodeMap.put(dataNode.getName(), jdbcDataNode);
            }
        }
    }


    public GBeanProviders getProvider() {
        return providers;
    }

    public TransactionSession createTransactionSession(GThread gThread) {
        if (isJTA) {
            return new JTATransactionSessionImpl(datasourceProvider.createUserTransaction(), gThread);
        } else {
            return new LocalTransactionSessionImpl(gThread);
        }
    }

    public DatasourceProvider getDatasourceProvider() {
        return datasourceProvider;
    }

    public <T extends ConfigurableRoot> T getConfig(ConfigFile configEnum) {
        ConfigurableRoot config = this.config.getConfig(configEnum);
        return (T) config;
    }

    public <K extends BindThreadKey, T extends BindThreadCallback> boolean run(K key, T processTask) {
      return   gThreadPool.run(key, processTask);
    }

    public Map<String, Object> getDefContext() {
        return defContext;
    }

    public int getMaxThread() {
        return jdbcDriverRootConfig.getMaxThread();
    }

    public int getWaitTaskTimeout() {
        return jdbcDriverRootConfig.getWaitTaskTimeout();
    }

    public String getTimeUnit() {
        return jdbcDriverRootConfig.getTimeUnit();
    }

    public int getMaxPengdingLimit() {
        return jdbcDriverRootConfig.getMaxPengdingLimit();
    }
}