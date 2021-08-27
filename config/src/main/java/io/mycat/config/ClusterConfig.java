package io.mycat.config;

import io.mycat.util.JsonUtil;
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
    @javax.validation.constraints.NotNull
    private String clusterType = "MASTER_SLAVE";
    @javax.validation.constraints.NotNull
    private String switchType = "SWITCH";
    @javax.validation.constraints.NotNull
    private String readBalanceType = "BALANCE_ALL";
    @javax.validation.constraints.NotNull
    private String name;
    private String readBalanceName;
    private String writeBalanceName;
    @javax.validation.constraints.NotNull
    private List<String> masters = new ArrayList<>();
    private List<String> replicas = new ArrayList<>();
    private HeartbeatConfig heartbeat = HeartbeatConfig.builder()
            .minSwitchTimeInterval(300)
            .heartbeatTimeout(1000)
            .slaveThreshold(0)
            .maxRetryCount(3)
            .build();
    private Integer maxCon = 2000;
    private TimerConfig timer = null;

    public ClusterConfig() {
    }

    public static void main(String[] args) {
        ClusterConfig clusterConfig = new ClusterConfig();
        System.out.println(JsonUtil.toJson(clusterConfig));
    }

    public List<String> allDatasources() {
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