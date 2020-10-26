package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@AllArgsConstructor
@Data
@EqualsAndHashCode
public class ClusterConfig {
    private String replicaType;
    private String switchType;
    private String readBalanceType;
    private String name;
    private String readBalanceName;
    private String writeBalanceName;
    private List<String> masters;
    private List<String> replicas;
    private HeartbeatConfig heartbeat;
    private Integer maxCon;
    private TimerConfig timer = null;

    public ClusterConfig() {
    }

    public List<String> getAllDatasources() {
        if (masters == null) {
            masters = Collections.emptyList();
        }
        if (replicas == null) {
            replicas = Collections.emptyList();
        }
        ArrayList<String> nodes = new ArrayList<>(masters.size() + replicas.size());
        nodes.addAll(masters);
        nodes.addAll(replicas);
        return nodes;
    }

}