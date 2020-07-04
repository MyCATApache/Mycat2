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
package io.mycat.replica;

import com.rits.cloning.Cloner;
import io.mycat.ConfigProvider;
import io.mycat.MycatConfig;
import io.mycat.RootHelper;
import io.mycat.config.ClusterRootConfig;
import io.mycat.plug.loadBalance.LoadBalanceInfo;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * @author : chenjunwen date Date : 2019年05月15日 21:34
 */

public class ReplicaDataSourceSelector implements LoadBalanceInfo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaDataSourceSelector.class);
    protected final String name;
    protected final ConcurrentHashMap<String, PhysicsInstanceImpl> datasourceMap = new ConcurrentHashMap<>();
    protected final BalanceType balanceType;
    private final int maxRequestCount;
    protected final ReplicaSwitchType switchType;
    protected final ReplicaType type;
    protected final LoadBalanceStrategy defaultReadLoadBalanceStrategy;
    protected final LoadBalanceStrategy defaultWriteLoadBalanceStrategy;
    protected final List<PhysicsInstanceImpl> writeDataSourceList = new CopyOnWriteArrayList<>();//只能被getWriteDataSource读取
    protected final List<PhysicsInstanceImpl> readDataSource = new CopyOnWriteArrayList<>();

    private final static boolean DEFAULT_SELECT_AS_READ = true;
    private final static boolean DEFAULT_ALIVE = false;

    public ReplicaDataSourceSelector(String name, BalanceType balanceType, ReplicaType type, int maxRequestCount,
                                     ReplicaSwitchType switchType, LoadBalanceStrategy defaultReadLoadBalanceStrategy,
                                     LoadBalanceStrategy defaultWriteLoadBalanceStrategy) {
        this.name = name;
        this.balanceType = balanceType;
        this.maxRequestCount = maxRequestCount;
        this.switchType = switchType;
        this.type = type;
        this.defaultReadLoadBalanceStrategy = defaultReadLoadBalanceStrategy;
        this.defaultWriteLoadBalanceStrategy = defaultWriteLoadBalanceStrategy;
        Objects.requireNonNull(balanceType, "balanceType is null");
    }

    /**
     * @param datasourceList
     * @return
     */
    private List<PhysicsInstanceImpl> getDataSource(Collection<PhysicsInstanceImpl> datasourceList) {
        int max = this.maxRequestCount();
        if (max < Integer.MAX_VALUE) {
            if (max <= datasourceList.stream().mapToInt(i -> i.getSessionCounter()).sum()) {
                return Collections.emptyList();
            }
        }
        if (datasourceList.isEmpty()) return Collections.emptyList();
        List<PhysicsInstanceImpl> result = datasourceList.stream().filter(mySQLDatasource -> mySQLDatasource.isAlive() && mySQLDatasource
                .asSelectRead()).collect(Collectors.toList());
        return result.isEmpty() ? Collections.emptyList() : result;
    }

    public synchronized PhysicsInstanceImpl register(String dataSourceName, InstanceType type,
                                                     int weight) {
        PhysicsInstanceImpl physicsInstance = datasourceMap.computeIfAbsent(dataSourceName,
                dataSourceName1 -> new PhysicsInstanceImpl(dataSourceName, type, DEFAULT_ALIVE,
                        DEFAULT_SELECT_AS_READ, weight,
                        ReplicaDataSourceSelector.this));
        if (type.isReadType()) {
            if (!this.readDataSource.contains(physicsInstance)) {
                this.readDataSource.add(physicsInstance);
            }
        }
        if (type.isWriteType()) {
            if (!this.writeDataSourceList.contains(physicsInstance)) {
                this.writeDataSourceList.add(physicsInstance);
            }
        }
        physicsInstance.notifyChangeAlive(false);
        physicsInstance.notifyChangeSelectRead(false);
        switch (this.type) {
            case SINGLE_NODE:
            case MASTER_SLAVE:
                break;
            case GARELA_CLUSTER:
                break;
            case NONE:
                break;
        }
        return physicsInstance;
    }


    public List getDataSourceByLoadBalacneType() {
        switch (this.balanceType) {
            case BALANCE_ALL:
                return getDataSource(this.datasourceMap.values());
            case BALANCE_NONE:
                return getWriteDataSource();
            case BALANCE_ALL_READ:
                return getDataSource(this.readDataSource);
            case BALANCE_READ_WRITE:
                List<PhysicsInstanceImpl> dataSource = getDataSource(this.readDataSource);
                return (dataSource.isEmpty()) ? getDataSource(getWriteDataSource()) : dataSource;
            default:
                return Collections.emptyList();
        }
    }

    public List getWriteDataSource() {
        switch (type) {
            case SINGLE_NODE:
            case MASTER_SLAVE:
                return Collections.singletonList(this.writeDataSourceList.get(0));
            case GARELA_CLUSTER:
            case NONE:
            default:
                return getDataSource(this.writeDataSourceList);
        }
    }

    public synchronized boolean switchDataSourceIfNeed() {
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

    private synchronized boolean switchWriteDataSource() {
        switch (type) {
            case SINGLE_NODE:
            case MASTER_SLAVE:
                Map<Boolean, List<PhysicsInstanceImpl>> map = this.writeDataSourceList.stream()
                        .filter(c -> c.getType().isWriteType()).collect(Collectors.groupingBy(k -> k.isAlive()));
                List<PhysicsInstanceImpl> first = map.getOrDefault(Boolean.TRUE, Collections.emptyList());
                List<PhysicsInstanceImpl> tail = map.getOrDefault(Boolean.FALSE, Collections.emptyList());
                ArrayList<PhysicsInstanceImpl> objects = new ArrayList<>();
                objects.addAll(first);
                objects.addAll(tail);
                return switchMaster(objects);
            case GARELA_CLUSTER:
                return switchMultiMaster();
            case NONE:
            default:
                return false;
        }
    }

    private synchronized boolean switchMultiMaster() {
        return switchMaster(this.writeDataSourceList.stream()
                .filter(datasource -> datasource.isAlive() && datasource.getType().isWriteType())
                .collect(Collectors.toList()));
    }


    private synchronized boolean switchReadDataSource() {
        return switchReadDatasource(this.datasourceMap.values().stream()
                .filter(c -> c.getType().isReadType() && c.isAlive()).collect(Collectors.toList()));
    }


    public PhysicsInstance getDataSource(boolean runOnMaster,
                                         LoadBalanceStrategy strategy) {
        return runOnMaster ? ReplicaSelectorRuntime.INSTANCE.getWriteDatasource(strategy, this)
                : ReplicaSelectorRuntime.INSTANCE.getDatasource(strategy, this);
    }

    private synchronized boolean switchReadDatasource(List<PhysicsInstanceImpl> newReadDataSource) {
        return switchNode((List) this.readDataSource, newReadDataSource,"{} switch replica to {}");
    }

    private synchronized boolean switchMaster(List<PhysicsInstanceImpl> newWriteDataSource) {
        boolean b = switchNode((List) this.writeDataSourceList,newWriteDataSource,  "{} switch master to {}");
        if (b) {
            updateFile(newWriteDataSource);
        }
        return b;
    }

    private synchronized boolean switchNode( List<PhysicsInstanceImpl> oldWriteDataSource,List<PhysicsInstanceImpl> newWriteDataSource, String message) {
        if (new ArrayList<>(oldWriteDataSource).equals(new ArrayList<>(newWriteDataSource))) {
            return false;
        }
        List<PhysicsInstanceImpl> backup = new ArrayList<>(oldWriteDataSource);
        CollectionUtil.safeUpdateByUpdateOrder(oldWriteDataSource, newWriteDataSource);
        LOGGER.info(message, backup, newWriteDataSource);
        return true;
    }

    private void updateFile(List<PhysicsInstanceImpl> newWriteDataSource) {
        ConfigProvider configProvider = RootHelper.INSTANCE.getConfigProvider();
        MycatConfig config = Cloner.standard().deepClone(configProvider.currentConfig());
        ClusterRootConfig.ClusterConfig clusterConfig = config.getCluster().getClusters().stream().filter(i -> getName().equals(i.getName())).findFirst().get();
        List<String> collect = newWriteDataSource.stream().map(i -> i.getName()).collect(Collectors.toList());
        clusterConfig.setMasters(collect);
        configProvider.reportReplica(clusterConfig.getName(), collect);
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public int maxRequestCount() {
        return maxRequestCount;
    }

    public ReplicaSwitchType getSwitchType() {
        return switchType;
    }

    public void unregister(String datasourceName) {
        datasourceMap.remove(datasourceName);
        writeDataSourceList.removeIf((i) -> i.getName().equals(datasourceName));
        readDataSource.removeIf((i) -> i.getName().equals(datasourceName));
    }

    public Map<String, PhysicsInstance> getRawDataSourceMap() {
        return Collections.unmodifiableMap(this.datasourceMap);
    }

    public BalanceType getBalanceType() {
        return balanceType;
    }

    public LoadBalanceStrategy getDefaultReadLoadBalanceStrategy() {
        return defaultReadLoadBalanceStrategy;
    }

    public LoadBalanceStrategy getDefaultWriteLoadBalanceStrategy() {
        return defaultWriteLoadBalanceStrategy;
    }

    public List<PhysicsInstanceImpl> getReadDataSource() {
        return Collections.unmodifiableList(readDataSource);
    }
}
