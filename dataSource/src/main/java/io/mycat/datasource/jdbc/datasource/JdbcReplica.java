/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.datasource.jdbc.datasource;

import io.mycat.beans.mycat.MycatReplica;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.GBeanProviders;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.replica.ReplicaDataSourceSelector;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.replica.ReplicaSwitchType;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
/**
 * @author Junwen Chen
 **/
public class JdbcReplica implements MycatReplica {

  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(JdbcReplica.class);
  protected final ReplicaDataSourceSelector selector;
  private final JdbcConnectionManager dataSourceManager;
  private final GRuntime runtime;
  private final ReplicaConfig replicaConfig;
  private final List<JdbcDataSource> dataSources;

  public JdbcReplica(GRuntime runtime,
      ReplicaConfig replicaConfig,
      Set<Integer> writeIndex, List<DatasourceConfig> datasourceConfigList,
      DatasourceProvider provider) {
    this.runtime = runtime;
    this.replicaConfig = replicaConfig;
    this.dataSources = getJdbcDataSources(datasourceConfigList);
    this.selector = ReplicaSelectorRuntime.INSTCANE.getDataSourceSelector(replicaConfig.getName());
    this.dataSourceManager = new JdbcConnectionManager(runtime, provider,
        dataSources);
  }

  private List<JdbcDataSource> getJdbcDataSources(
      List<DatasourceConfig> datasourceConfigList) {
    GBeanProviders provider = runtime.getProvider();
    ArrayList<JdbcDataSource> dataSources = new ArrayList<>();
    for (int i = 0; i < datasourceConfigList.size(); i++) {
      DatasourceConfig datasourceConfig = datasourceConfigList.get(i);
      if (datasourceConfig.getDbType() != null && datasourceConfig.getUrl() != null) {
        JdbcDataSource jdbcDataSource = provider
            .createJdbcDataSource(runtime, i, datasourceConfig, this);
        dataSources.add(jdbcDataSource);
      } else {
        dataSources.add(null);
      }
    }
    return dataSources;
  }

  public JdbcDataSource getDataSourceByBalance(JdbcDataSourceQuery query) {
    boolean runOnMaster = false;
    LoadBalanceStrategy strategy = null;
    if (query != null) {
      runOnMaster = query.isRunOnMaster();
      strategy = query.getStrategy();
    }
    return dataSources.get(selector.getDataSource(runOnMaster, strategy).getIndex());
  }


  public String getName() {
    return replicaConfig.getName();
  }

  @Override
  public ReplicaConfig getReplicaConfig() {
    return replicaConfig;
  }

  @Override
  public boolean switchDataSourceIfNeed() {
    return selector.switchDataSourceIfNeed();
  }

  public List<JdbcDataSource> getDatasourceList() {
    return this.dataSourceManager.getDatasourceList();
  }

  public boolean isMaster(JdbcDataSource jdbcDataSource) {
    return selector.isMaster(jdbcDataSource.getIndex());
  }

  private Connection getConnection(JdbcDataSource dataSource) {
    return dataSourceManager.getConnection(dataSource);
  }

  public DsConnection getDefaultConnection(JdbcDataSource dataSource) {
    Connection connection = getConnection(dataSource);
    return new DefaultConnection(connection, dataSource, true,
        Connection.TRANSACTION_REPEATABLE_READ,
        dataSourceManager);
  }

  public DsConnection getConnection(JdbcDataSource dataSource, boolean autocommit,
      int transactionIsolation) {
    Connection connection = getConnection(dataSource);
    return new DefaultConnection(connection, dataSource, autocommit,
        transactionIsolation,
        dataSourceManager);
  }

  public ReplicaSwitchType getSwitchType() {
    return selector.getSwitchType();
  }
}