/**
 * Copyright (C) <2021>  <chen junwen>
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

import io.mycat.*;
import io.mycat.config.TimerConfig;
import io.mycat.plug.loadBalance.LoadBalanceInfo;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.plug.loadBalance.SessionCounter;
import io.mycat.replica.heartbeat.HeartbeatFlow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * @author : chenjunwen date Date : 2019年05月15日 21:34
 */

public class ReplicaDataSourceSelector implements LoadBalanceInfo, Closeable, ReplicaSelector {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaDataSourceSelector.class);
    protected final String name;
    protected final ConcurrentHashMap<String, PhysicsInstance> datasourceMap = new ConcurrentHashMap<>();
    protected final BalanceType balanceType;
    private final int maxRequestCount;
    protected final ReplicaSwitchType switchType;
    protected final ReplicaType type;
    protected final LoadBalanceStrategy defaultReadLoadBalanceStrategy;
    protected final LoadBalanceStrategy defaultWriteLoadBalanceStrategy;
    private ReplicaSelectorRuntime replicaSelectorRuntime;
    protected volatile List<PhysicsInstance> writeDataSourceList = new CopyOnWriteArrayList<>();//只能被getWriteDataSource读取
    protected volatile List<PhysicsInstance> readDataSource = new CopyOnWriteArrayList<>();

    private final static boolean DEFAULT_SELECT_AS_READ = true;
    private final static boolean DEFAULT_ALIVE = false;
    private final io.mycat.replica.ScheduledHanlde scheduled;
    private String dbType;


    public ReplicaDataSourceSelector(String name, String dbType, BalanceType balanceType, ReplicaType type, int maxRequestCount,
                                     ReplicaSwitchType switchType, LoadBalanceStrategy defaultReadLoadBalanceStrategy,
                                     LoadBalanceStrategy defaultWriteLoadBalanceStrategy,
                                     TimerConfig timer, ReplicaSelectorRuntime replicaSelectorRuntime) {
        this.name = name;
        this.dbType = dbType;
        this.balanceType = balanceType;
        this.maxRequestCount = maxRequestCount;
        this.switchType = switchType;
        this.type = type;
        this.defaultReadLoadBalanceStrategy = defaultReadLoadBalanceStrategy;
        this.defaultWriteLoadBalanceStrategy = defaultWriteLoadBalanceStrategy;
        this.replicaSelectorRuntime = replicaSelectorRuntime;
        Objects.requireNonNull(balanceType, "balanceType is null");

        if (timer != null) {
            this.scheduled = replicaSelectorRuntime.getScheduleProvider().scheduleAtFixedRate(() -> {
                String replicaName = name;
                Enumeration<String> keys = datasourceMap.keys();
                while (keys.hasMoreElements()) {
                    String datasourceName = keys.nextElement();
                    String key = replicaName + "." + datasourceName;
                    HeartbeatFlow heartbeatFlow = replicaSelectorRuntime.getHeartbeatDetectorMap().get(key);
                    if (heartbeatFlow != null) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("heartbeat:{}", key);
                        }
                        heartbeatFlow.heartbeat();
                    }
                }
            }, timer.getInitialDelay(), timer.getPeriod(), TimeUnit.valueOf(timer.getTimeUnit()));
        } else {
            this.scheduled = null;
        }
    }

    @Override
    public void finalize() throws Throwable {
        super.finalize();
        close();
    }

    /**
     * @param datasourceList
     * @return
     */
    private List<PhysicsInstance> getDataSource(Collection<PhysicsInstance> datasourceList) {
        if (datasourceList.isEmpty()) return Collections.emptyList();
        List<PhysicsInstance> result = datasourceList.stream().filter(mySQLDatasource -> mySQLDatasource.isAlive() && mySQLDatasource
                .asSelectRead()).collect(Collectors.toList());
        return result.isEmpty() ? Collections.emptyList() : result;
    }

    public synchronized PhysicsInstance register(String dataSourceName, InstanceType type,
                                                 int weight, SessionCounter sessionCounter) {
        PhysicsInstance physicsInstance = datasourceMap.computeIfAbsent(dataSourceName,
                dataSourceName1 -> new PhysicsInstanceImpl(dataSourceName, type, DEFAULT_ALIVE,
                        DEFAULT_SELECT_AS_READ, weight, sessionCounter,
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
                return getWriteDataSourceByReplicaType();
            case BALANCE_ALL_READ:
                return getDataSource(this.readDataSource);
            case BALANCE_READ_WRITE:
                List<PhysicsInstance> dataSource = getDataSource(this.readDataSource);
                return (dataSource.isEmpty()) ? getDataSource(getWriteDataSourceByReplicaType()) : dataSource;
            default:
                return Collections.emptyList();
        }
    }

    public List getWriteDataSourceByReplicaType() {
        switch (type) {
            case SINGLE_NODE:
            case MASTER_SLAVE:
                if (this.writeDataSourceList.isEmpty()) {
                    return Collections.emptyList();
                }
                return Collections.singletonList(this.writeDataSourceList.get(0));
            case GARELA_CLUSTER:
            case NONE:
            default:
                return getDataSource(this.writeDataSourceList);
        }
    }

    private synchronized void switchWriteDataSource() {
        switch (type) {
            case SINGLE_NODE:
            case MASTER_SLAVE:
                Map<Boolean, List<PhysicsInstance>> map = this.writeDataSourceList.stream()
                        .filter(c -> c.getType().isWriteType()).collect(Collectors.groupingBy(k -> k.isAlive()));
                List<PhysicsInstance> first = map.getOrDefault(Boolean.TRUE, Collections.emptyList());
                List<PhysicsInstance> tail = map.getOrDefault(Boolean.FALSE, Collections.emptyList());
                if (!tail.isEmpty()) {
                    ArrayList<PhysicsInstance> newWriteDataSource = new ArrayList<>();
                    newWriteDataSource.addAll(first);
                    newWriteDataSource.addAll(tail);
                    LOGGER.info("{} switch master to {}", this.writeDataSourceList, this.writeDataSourceList = new CopyOnWriteArrayList<>(newWriteDataSource));
                    updateFile(newWriteDataSource);
                }
            case GARELA_CLUSTER:
            case NONE:
            default:

        }
    }


    private void updateFile(List<PhysicsInstance> newWriteDataSource) {
        if (MetaClusterCurrent.exist(ReplicaReporter.class)) {
            ReplicaReporter replicaReporter = MetaClusterCurrent.wrapper(ReplicaReporter.class);
            Map<String, List<String>> state = replicaSelectorRuntime.getState();
            replicaReporter.reportReplica(state);
        } else {
            LOGGER.error("not found ReplicaReporter");
        }
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
        return (this.datasourceMap);
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

    public List<PhysicsInstance> getReadDataSourceByReplica() {
        return Collections.unmodifiableList(readDataSource);
    }

    @Override
    public synchronized void close() {
        if (scheduled != null) {
            scheduled.close();
        }
    }

    public Collection<String> getAllDataSources() {
        return this.datasourceMap.keySet();
    }

    public String getDbType() {
        return dbType;
    }

    public synchronized void updateInstanceStatus(String dataSource,
                                                  boolean alive,
                                                  boolean selectAsRead,
                                                  boolean master) {
        PhysicsInstance physicsInstance = datasourceMap.get(dataSource);
        if (physicsInstance != null) {
            physicsInstance.notifyChangeAlive(alive);
            physicsInstance.notifyChangeSelectRead(selectAsRead);
        }
    }

    public ReplicaType getType() {
        return type;
    }

    @Override
    public void notifySwitchReplicaDataSource() {
        switch (this.switchType) {
            case SWITCH:
                switchWriteDataSource();
            case NOT_SWITCH:
            default:
        }
    }

    public synchronized void removeWriteDataSource(String dataSource) {
        PhysicsInstance physicsInstance = Objects.requireNonNull(datasourceMap.get(dataSource));
        writeDataSourceList.remove(physicsInstance);
    }


    public synchronized void addWriteDataSource(String dataSource) {
        PhysicsInstance physicsInstance = datasourceMap.get(dataSource);
        if (!writeDataSourceList.contains(physicsInstance)) {
            writeDataSourceList.add(physicsInstance);
        }

    }

    public synchronized void addReadDataSource(String dataSource) {
        PhysicsInstance physicsInstance = datasourceMap.get(dataSource);
        if (!readDataSource.contains(physicsInstance)) {
            readDataSource.add(physicsInstance);
        }

    }

    public synchronized void removeReadDataSource(String dataSource) {
        readDataSource.remove(Objects.requireNonNull(datasourceMap.get(dataSource)));
    }

}
