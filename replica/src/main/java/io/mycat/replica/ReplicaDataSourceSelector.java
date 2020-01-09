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

import io.mycat.MycatException;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.loadBalance.LoadBalanceInfo;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author : chenjunwen date Date : 2019年05月15日 21:34
 */
public class ReplicaDataSourceSelector implements LoadBalanceInfo {

    protected static final MycatLogger LOGGER = MycatLoggerFactory
            .getLogger(ReplicaDataSourceSelector.class);
    protected final String name;
    protected final ConcurrentHashMap<String, PhysicsInstanceImpl> datasourceMap = new ConcurrentHashMap<>();
    protected final BalanceType balanceType;
    protected final ReplicaSwitchType switchType;
    protected final ReplicaType type;
    protected final LoadBalanceStrategy defaultReadLoadBalanceStrategy;
    protected final LoadBalanceStrategy defaultWriteLoadBalanceStrategy;
    protected final ConcurrentHashMap<String, PhysicsInstanceImpl> writeDataSource = new ConcurrentHashMap<>();
    protected final ConcurrentHashMap<String, PhysicsInstanceImpl> readDataSource = new ConcurrentHashMap<>();

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

    private static List<PhysicsInstanceImpl> getDataSource(Collection<PhysicsInstanceImpl> datasourceList) {
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
            this.readDataSource.put(physicsInstance.getName(), physicsInstance);
        }
        if (type.isWriteType()) {
            this.writeDataSource.put(physicsInstance.getName(), physicsInstance);
            physicsInstance.notifyChangeAlive(false);
            physicsInstance.notifyChangeSelectRead(false);
        }
        switch (this.type) {
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
        return physicsInstance;
    }


    public List getDataSourceByLoadBalacneType() {
        switch (this.balanceType) {
            case BALANCE_ALL:
                return getDataSource(this.datasourceMap.values());
            case BALANCE_NONE:
                return getWriteDataSource();
            case BALANCE_ALL_READ:
                return getDataSource(this.readDataSource.values());
            case BALANCE_READ_WRITE:
                List<PhysicsInstanceImpl> dataSource = getDataSource(this.readDataSource.values());
                return (dataSource.isEmpty()) ? getDataSource(this.writeDataSource.values()) : dataSource;
            default:
                return Collections.emptyList();
        }
    }

    public List getWriteDataSource() {
        return getDataSource(this.writeDataSource.values());
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
                return switchSingleMaster();
            case GARELA_CLUSTER:
                return switchMultiMaster();
            case NONE:
            default:
                return false;
        }
    }

    private synchronized boolean switchMultiMaster() {
        return switchMaster(this.datasourceMap.values().stream()
                .filter(datasource -> datasource.isAlive() && datasource.getType().isWriteType())
                .collect(Collectors.toMap(k -> k.getName(), v -> v)));
    }


    private synchronized boolean switchSingleMaster() {
        return switchMaster(this.datasourceMap.values().stream()
                .filter(c -> c.getType().isWriteType() && c.isAlive()).collect(Collectors.toMap(k -> k.getName(), v -> v)));
    }

    private synchronized boolean switchReadDataSource() {
        return switchReplica(this.datasourceMap.values().stream()
                .filter(c -> c.getType().isReadType() && c.isAlive()).collect(Collectors.toMap(k -> k.getName(), v -> v)));
    }


    public PhysicsInstance getDataSource(boolean runOnMaster,
                                         LoadBalanceStrategy strategy) {
        return runOnMaster ? ReplicaSelectorRuntime.INSTANCE.getWriteDatasource(strategy, this)
                : ReplicaSelectorRuntime.INSTANCE.getDatasource(strategy, this);
    }

    private synchronized boolean switchReplica(Map<String, PhysicsInstanceImpl> newReadDataSource) {
        return switchNode(newReadDataSource, this.readDataSource, "{} switch replica to {}");
    }

    private synchronized boolean switchMaster(Map<String, PhysicsInstanceImpl> newWriteDataSource) {
        return switchNode(newWriteDataSource, this.writeDataSource, "{} switch master to {}");
    }

    private synchronized boolean switchNode(Map<String, PhysicsInstanceImpl> newWriteDataSource, Map<String, PhysicsInstanceImpl> oldWriteDataSource, String message) {
        if (this.writeDataSource.equals(newWriteDataSource)) {
            return false;
        }
        Map<String, PhysicsInstanceImpl> backup = new HashMap<>(this.writeDataSource);
        oldWriteDataSource.replaceAll((s, physicsInstance) -> newWriteDataSource.get(s));
        oldWriteDataSource.putAll(newWriteDataSource);
        LOGGER.info(message, backup, newWriteDataSource);
        return true;
    }

    @Override
    public String getName() {
        return this.name;
    }


    public boolean isMaster(String name) {
        return this.writeDataSource.containsKey(name);
    }

    public ReplicaSwitchType getSwitchType() {
        return switchType;
    }

    public void unregister(String datasourceName) {
        datasourceMap.remove(datasourceName);
        writeDataSource.remove(datasourceName);
        readDataSource.remove(datasourceName);
    }
}
