package io.mycat.replica;

import io.mycat.ConfigRuntime;
import io.mycat.MycatException;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.loadBalance.LoadBalanceInfo;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ReplicaDataSourceSelector implements LoadBalanceInfo {

  protected static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(ReplicaDataSourceSelector.class);
  protected final String name;
  protected final ArrayList<PhysicsInstanceImpl> datasourceList = new ArrayList<>();
  protected final Map<String, PhysicsInstanceImpl> datasourceMap = new ConcurrentHashMap<>();
  protected final BalanceType balanceType;
  protected final ReplicaSwitchType switchType;
  protected final ReplicaType type;
  protected final LoadBalanceStrategy defaultReadLoadBalanceStrategy;
  protected final LoadBalanceStrategy defaultWriteLoadBalanceStrategy;
  protected volatile CopyOnWriteArrayList<PhysicsInstanceImpl> writeDataSource = new CopyOnWriteArrayList<>();
  protected volatile CopyOnWriteArrayList<PhysicsInstanceImpl> readDataSource = new CopyOnWriteArrayList<>();

  private final static boolean DEFAULT_SELECT_AS_READ = true;
  private final static boolean DEFAULT_ALIVE = false;

  public ReplicaDataSourceSelector(String name, BalanceType balanceType, ReplicaType type,
      ReplicaSwitchType switchType, LoadBalanceStrategy defaultReadLoadBalanceStrategy,
      LoadBalanceStrategy defaultWriteLoadBalanceStrategy) {
    this.name = name;
    this.balanceType = balanceType;
    this.switchType = switchType;
    this.type = type;
    this.defaultReadLoadBalanceStrategy = defaultReadLoadBalanceStrategy;
    this.defaultWriteLoadBalanceStrategy = defaultWriteLoadBalanceStrategy;
    Objects.requireNonNull(balanceType, "balanceType is null");
  }

  private static List<PhysicsInstanceImpl> getDataSource(List<PhysicsInstanceImpl> datasourceList) {
    if (datasourceList.isEmpty()) {
      return Collections.emptyList();
    }
    if (datasourceList.size() == 1) {
      PhysicsInstanceImpl datasource = datasourceList.get(0);
      return datasource.isAlive() && datasource.asSelectRead() ? Collections
          .singletonList(datasource)
          : Collections.emptyList();
    }
    List<PhysicsInstanceImpl> result = new ArrayList<>(datasourceList.size());
    for (PhysicsInstanceImpl mySQLDatasource : datasourceList) {
      if (mySQLDatasource.isAlive() && mySQLDatasource
          .asSelectRead()) {
        result.add(mySQLDatasource);
      }
    }
    return result.isEmpty() ? Collections.emptyList() : result;
  }

  public PhysicsInstanceImpl register(int index, String dataSourceName, InstanceType type,
      int weight) {
    return datasourceMap.computeIfAbsent(dataSourceName,
        new Function<String, PhysicsInstanceImpl>() {
          @Override
          public PhysicsInstanceImpl apply(String dataSourceName) {
            return new PhysicsInstanceImpl(index, dataSourceName, type, DEFAULT_ALIVE,
                DEFAULT_SELECT_AS_READ, weight,
                ReplicaDataSourceSelector.this);
          }
        });
  }

  public void registerFininshed() {
    ArrayList<PhysicsInstanceImpl> read = new ArrayList<>();
    ArrayList<PhysicsInstanceImpl> write = new ArrayList<>();
    List<PhysicsInstanceImpl> collect = datasourceMap.values().stream().sorted(Comparator.comparing(
        PhysicsInstanceImpl::getIndex)).collect(
        Collectors.toList());
    datasourceList.addAll(collect);
    for (PhysicsInstanceImpl instance : datasourceList) {
      InstanceType type = instance.getType();
      if (type.isReadType()) {
        read.add(instance);
      }
      if (type.isWriteType()) {
        write.add(instance);
      }
    }
    readDataSource = new CopyOnWriteArrayList<>(read);
    writeDataSource = new CopyOnWriteArrayList<>(write);

    for (PhysicsInstanceImpl physicsInstance : writeDataSource) {
      physicsInstance.notifyChangeAlive(true);
      physicsInstance.notifyChangeSelectRead(true);
    }

    switch (type) {
      case SINGLE_NODE:
      case MASTER_SLAVE:
        if (writeDataSource.size() != 1) {
          throw new MycatException(
              "replica:{} SINGLE_NODE and MASTER_SLAVE only support one master index.",
              name);
        }
        break;
      case GARELA_CLUSTER:
        break;
      case NONE:
        break;
    }
  }

  public List getDataSourceByLoadBalacneType() {
    switch (this.balanceType) {
      case BALANCE_ALL:
        return getDataSource(this.datasourceList);
      case BALANCE_NONE:
        return getWriteDataSource();
      case BALANCE_ALL_READ:
        return getDataSource(this.readDataSource);
      case BALANCE_READ_WRITE:
        List<PhysicsInstanceImpl> dataSource = getDataSource(this.readDataSource);
        return (dataSource.isEmpty()) ? getDataSource(this.writeDataSource) : dataSource;
      default:
        return Collections.emptyList();
    }
  }

  public List getWriteDataSource() {
    return getDataSource(this.writeDataSource);
  }

  public boolean switchDataSourceIfNeed() {
    boolean readDataSource = switchReadDataSource();
    switch (this.switchType) {
      case SWITCH:
        boolean writeDataSource = switchWriteDataSource();
        return readDataSource || writeDataSource;
      case NOT_SWITCH:
      default:
        return readDataSource;
    }
  }

  private boolean switchWriteDataSource() {
    switch (type) {
      case SINGLE_NODE:
      case MASTER_SLAVE:
        return switchSingleMaster();
      case GARELA_CLUSTER:
        return switchMultiMaster();
      case NONE:
      default:
        return false;
    }
  }

  private boolean switchMultiMaster() {
    CopyOnWriteArrayList<PhysicsInstanceImpl> oldWriteDataSource = this.writeDataSource;
    List<PhysicsInstanceImpl> newWriteDataSource = new CopyOnWriteArrayList<>(
        this.datasourceList.stream()
            .filter(datasource -> datasource.isAlive() && datasource.getType().isWriteType())
            .collect(Collectors.toList()));
    if (oldWriteDataSource.equals(newWriteDataSource)) {
      return false;
    }
    this.writeDataSource = new CopyOnWriteArrayList<>(newWriteDataSource);
    LOGGER.info("{} switch master to {}", oldWriteDataSource, newWriteDataSource);
    return true;
  }

  private boolean switchSingleMaster() {
    CopyOnWriteArrayList<PhysicsInstanceImpl> oldWriteDataSource = this.writeDataSource;
    if (oldWriteDataSource.isEmpty() || (oldWriteDataSource.size() == 1 && oldWriteDataSource.get(0)
        .isAlive())) {
      return false;
    }
    CopyOnWriteArrayList<PhysicsInstanceImpl> newWriteDataSource = new CopyOnWriteArrayList<>(
        this.datasourceList.stream()
            .filter(c -> c.getType().isWriteType() && c.isAlive())
            .collect(Collectors.toList()));

    if (oldWriteDataSource.equals(newWriteDataSource)) {
      return false;
    }
    ConfigRuntime.INSTCANE.modifyReplicaMasterIndexes(getName(),
        oldWriteDataSource.stream().map(i -> i.getIndex()).collect(
            Collectors.toList()),
        newWriteDataSource.stream().map(i -> i.getIndex()).collect(Collectors.toList()));
    this.writeDataSource = newWriteDataSource;
    LOGGER.info("{} switch master to {}", oldWriteDataSource, newWriteDataSource);
    return true;
  }

  private boolean switchReadDataSource() {
    CopyOnWriteArrayList<PhysicsInstanceImpl> oldReadDataSource = this.readDataSource;

    CopyOnWriteArrayList<PhysicsInstanceImpl> newReadDataSource = new CopyOnWriteArrayList<>(
        this.datasourceList.stream()
            .filter(c -> c.getType().isReadType() && c.isAlive())
            .collect(Collectors.toList()));

    if (oldReadDataSource.equals(newReadDataSource)) {
      return false;
    }
    this.readDataSource = newReadDataSource;
    LOGGER.info("{} switch master to {}", oldReadDataSource, newReadDataSource);
    return true;
  }

  public <T> T getDataSource(boolean runOnMaster,
      LoadBalanceStrategy strategy) {
    PhysicsInstanceImpl instance =
        runOnMaster ? ReplicaSelectorRuntime.INSTCANE.getWriteDatasource(strategy, this)
            : ReplicaSelectorRuntime.INSTCANE.getDatasource(strategy, this);
    return (T) instance;
  }

  @Override
  public String getName() {
    return this.name;
  }


  public boolean isMaster(int index) {
    return datasourceList.get(index).isMaster();
  }

  public ReplicaSwitchType getSwitchType() {
    return switchType;
  }
}
