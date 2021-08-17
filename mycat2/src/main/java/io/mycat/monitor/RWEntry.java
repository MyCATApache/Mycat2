package io.mycat.monitor;

import com.google.common.collect.ImmutableList;
import io.mycat.MetaClusterCurrent;
import io.mycat.config.ClusterConfig;
import io.mycat.config.MycatRouterConfig;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaSelector;
import io.mycat.replica.ReplicaSelectorManager;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Data
@ToString
@EqualsAndHashCode
public class RWEntry implements LogEntry {
    long master;
    long slave;
    boolean status;

    public RWEntry() {
    }

    public RWEntry(long master, long slave, boolean status) {
        this.master = master;
        this.slave = slave;
        this.status = status;
    }

    public static class Entry {
        public final AtomicLong SLAVE = new AtomicLong();
        public final AtomicLong MASTER = new AtomicLong();
    }

    public static final ConcurrentHashMap<String, Entry> map = (new ConcurrentHashMap<String, Entry>());


    public static void stat(String replica, boolean master) {
        Entry entry = map.computeIfAbsent(replica, s -> new Entry());
        if (master) {
            entry.MASTER.getAndIncrement();
        } else {
            entry.SLAVE.getAndIncrement();
        }
    }

    public static RWEntryMap snapshot() {
        ReplicaSelectorManager replicaSelectorManager = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
        MycatRouterConfig routerConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        Map<String, ClusterConfig> clusterConfigMap = routerConfig.getClusters().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));

        Map<String, ReplicaSelector> replicaMap = replicaSelectorManager.getReplicaMap();
        Map<String, RWEntry> rwEntryMap = new HashMap<>();
        for (Map.Entry<String, Entry> entry : map.entrySet()) {
            String name = entry.getKey();
            Entry value = entry.getValue();


            ////////////////////////////////////////////////////////////////
            boolean status = false;
            ReplicaSelector replicaSelector = replicaMap.get(name);
            if (replicaSelector != null) {

                ClusterConfig clusterConfig = clusterConfigMap.get(replicaSelector.getName());
                List<String> dsNames = (List) ImmutableList.builder()
                        .addAll(clusterConfig.getMasters())
                        .addAll(clusterConfig.getReplicas())
                        .build().stream().distinct().collect(Collectors.toList());

                int i = 0;
                for (; i < dsNames.size(); i++) {
                    String dsName = dsNames.get(i);

                    PhysicsInstance physicsInstance = replicaSelector.getRawDataSourceMap().get(dsName);
                    if (physicsInstance == null) {
                        break;
                    } else {
                        if (physicsInstance.isAlive()) {

                        } else {
                            break;
                        }
                    }

                }
                status = i == dsNames.size();
            } else {
                status = false;
            }
            rwEntryMap.put(name, new RWEntry(value.MASTER.get(), value.SLAVE.get(), status));
        }
        RWEntryMap res = new RWEntryMap();
        res.rwMap = rwEntryMap;


        return res;
    }

    public static void reset() {
        map.clear();
    }


    @Data
    public static class RWEntryMap {
        Map<String, RWEntry> rwMap = new HashMap<>();

        public RWEntryMap() {
        }
    }


}
