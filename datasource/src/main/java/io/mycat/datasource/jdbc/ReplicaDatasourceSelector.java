package io.mycat.datasource.jdbc;

import io.mycat.MycatException;
import io.mycat.beans.mycat.MycatDataSource;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicaConfig.BalanceTypeEnum;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.loadBalance.LoadBalanceInfo;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

public class ReplicaDatasourceSelector<T extends MycatDataSource> implements LoadBalanceInfo {

  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(
      ReplicaDatasourceSelector.class);
  protected final ReplicaConfig config;
  protected final List<T> datasourceList;
  protected final CopyOnWriteArrayList<T> writeDataSource = new CopyOnWriteArrayList<>(); //主节点默认为0
  protected final GridRuntime runtime;
  protected LoadBalanceStrategy defaultLoadBalanceStrategy;

  public ReplicaDatasourceSelector(GridRuntime runtime, ReplicaConfig replicaConfig,
      Set<Integer> writeIndex, List<T> datasourceList,
      LoadBalanceStrategy defaultLoadBalanceStrategy) {
    this.runtime = runtime;
    this.config = replicaConfig;
    Objects.requireNonNull(runtime);
    Objects.requireNonNull(replicaConfig);
    this.defaultLoadBalanceStrategy = defaultLoadBalanceStrategy;
    this.datasourceList = datasourceList;
    for (T jdbcDataSource : datasourceList) {
      int index = jdbcDataSource.getIndex();
      if (writeIndex.contains(index)) {
        switch (config.getRepType()) {
          case SINGLE_NODE:
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
    if (writeDataSource.isEmpty()) {
      throw new MycatException("writeDataSource list is empty");
    }
  }


  public List getDataSourceByLoadBalacneType() {
    BalanceTypeEnum balanceType = this.config.getBalanceType();
    Objects.requireNonNull(balanceType, "balanceType is null");
    switch (balanceType) {
      case BALANCE_ALL:
        List<T> list = new ArrayList<>(this.datasourceList.size());
        for (T datasource : this.datasourceList) {
          if (datasource.isAlive()) {
            list.add(datasource);
          }
        }
        return list;
      case BALANCE_NONE:
        return getMaster();
      case BALANCE_ALL_READ:
        List<T> result = new ArrayList<>(this.datasourceList.size());
        for (T mySQLDatasource : this.datasourceList) {
          if (mySQLDatasource.isAlive() && mySQLDatasource.asSelectRead()) {
            result.add(mySQLDatasource);
          }
        }
        return result;
      default:
        return Collections.EMPTY_LIST;
    }
  }

  public T getWriteDatasource(LoadBalanceStrategy strategy) {
    if (strategy == null) {
      strategy = this.defaultLoadBalanceStrategy;
    }
    List writeDataSource = this.writeDataSource;
    T datasource = (T) strategy
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
    T datasource;
    if (writeDataSource.isEmpty()) {
      return Collections.emptyList();
    }
    int size = writeDataSource.size();
    if (writeDataSource.size() == 1) {
      datasource = writeDataSource.get(0);
      return datasource.isAlive() ? Collections.singletonList(datasource) : Collections.emptyList();
    }
    ArrayList<MycatDataSource> datasources = new ArrayList<>(size);
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

  public boolean isMaster(MycatDataSource datasource) {
    return writeDataSource.contains(datasource);
  }
}