package io.mycat.datasource.jdbc;

import io.mycat.MycatException;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicaConfig.BalanceTypeEnum;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.loadBalance.LoadBalanceInfo;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.replica.MySQLDatasource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

public class JdbcReplicaDatasourceSelector implements LoadBalanceInfo {

  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(
      JdbcReplicaDatasourceSelector.class);
  protected final ReplicaConfig config;
  protected final List<JdbcDataSource> datasourceList;
  protected final CopyOnWriteArrayList<JdbcDataSource> writeDataSource = new CopyOnWriteArrayList<>(); //主节点默认为0
  protected final ProxyRuntime runtime;
  protected LoadBalanceStrategy defaultLoadBalanceStrategy;

  public JdbcReplicaDatasourceSelector(ProxyRuntime runtime, ReplicaConfig replicaConfig,
      Set<Integer> writeIndex) {
    this.runtime = runtime;
    this.config = replicaConfig;
    Objects.requireNonNull(runtime);
    Objects.requireNonNull(replicaConfig);

    defaultLoadBalanceStrategy = runtime
        .getLoadBalanceByBalanceName(replicaConfig.getBalanceName());

    datasourceList = getJdbcDatasourceList(replicaConfig);
    for (JdbcDataSource jdbcDataSource : datasourceList) {
      int index = jdbcDataSource.getIndex();
      if (writeIndex.contains(index)) {
        switch (config.getRepType()) {
          case SINGLE_NODE:
            break;
          case MASTER_SLAVE: {
            if (writeDataSource.isEmpty()) {
              writeDataSource.add(jdbcDataSource);
            } else {
              throw new MycatException(
                  "replica:{} SINGLE_NODE and MASTER_SLAVE only support one master index.",
                  replicaConfig.getName());
            }
            break;
          }
          case GARELA_CLUSTER:
            writeDataSource.add(jdbcDataSource);
            break;
        }
      }
    }
  }

  public static List<JdbcDataSource> getJdbcDatasourceList(ReplicaConfig replicaConfig) {
    List<DatasourceConfig> mysqls = replicaConfig.getMysqls();
    if (mysqls == null) {
      return Collections.emptyList();
    }
    List<JdbcDataSource> datasourceList = new ArrayList<>();
    for (int index = 0; index < mysqls.size(); index++) {
      DatasourceConfig datasourceConfig = mysqls.get(index);
      if (datasourceConfig.getDbType() != null) {
        datasourceList.add(new JdbcDataSource(index, datasourceConfig));
      }
    }
    return datasourceList;
  }

  public JdbcDataSource getDataSourceByBalance(JdbcDataSourceQuery query) {
    boolean runOnMaster = false;
    LoadBalanceStrategy strategy = null;

    if (query != null) {
      runOnMaster = query.isRunOnMaster();
      strategy = query.getStrategy();
    }

    if (strategy == null) {
      strategy = this.defaultLoadBalanceStrategy;
    }

    if (runOnMaster) {
      return getWriteDatasource(strategy);
    }
    JdbcDataSource datasource;
    List activeDataSource = getDataSourceByLoadBalacneType();
    datasource = (JdbcDataSource) strategy.select(this, activeDataSource);
    if (datasource == null) {
      datasource = getWriteDatasource(strategy);
      return datasource;
    }
    return datasource;
  }

  private List getDataSourceByLoadBalacneType() {
    BalanceTypeEnum balanceType = this.config.getBalanceType();
    Objects.requireNonNull(balanceType, "balanceType is null");
    switch (balanceType) {
      case BALANCE_ALL:
        List<JdbcDataSource> list = new ArrayList<>(this.datasourceList.size());
        for (JdbcDataSource datasource : this.datasourceList) {
          if (datasource.isAlive()) {
            list.add(datasource);
          }
        }
        return list;
      case BALANCE_NONE:
        return getMaster();
      case BALANCE_ALL_READ:
        List<JdbcDataSource> result = new ArrayList<>(this.datasourceList.size());
        for (JdbcDataSource mySQLDatasource : this.datasourceList) {
          if (mySQLDatasource.isAlive() && mySQLDatasource.asSelectRead()) {
            result.add(mySQLDatasource);
          }
        }
        return result;
      default:
        return Collections.EMPTY_LIST;
    }
  }

  private JdbcDataSource getWriteDatasource(LoadBalanceStrategy strategy) {
    if (strategy == null) {
      strategy = this.defaultLoadBalanceStrategy;
    }
    List writeDataSource = this.writeDataSource;
    JdbcDataSource datasource = (JdbcDataSource) strategy
        .select(this, writeDataSource);
    if (datasource == null || !datasource.isAlive()) {
      return null;
    }
    return datasource;
  }

  @Override
  public String getName() {
    return config.getName();
  }

  private List getMaster() {
    JdbcDataSource datasource;
    if (writeDataSource.isEmpty()) {
      return Collections.emptyList();
    }
    int size = writeDataSource.size();
    if (writeDataSource.size() == 1) {
      datasource = writeDataSource.get(0);
      return datasource.isAlive() ? Collections.singletonList(datasource) : Collections.emptyList();
    }
    ArrayList<JdbcDataSource> datasources = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      datasource = (writeDataSource.get(i));
      if (datasource.isAlive()) {
        datasources.add(datasource);
      } else {
        //writeDataSource.remove(i);
      }
    }
    return datasources;
  }

  public boolean isMaster(MySQLDatasource datasource) {
    return writeDataSource.contains(datasource);
  }
}