/**
 * Copyright (C) <2019>  <chen junwen,gaozhiwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * <p>
 * date: 12/26/2019
 *
 * @author: chenunwen
 */
@Data
@AllArgsConstructor
public class ClusterRootConfig {
    private List<ClusterConfig> clusters = new ArrayList<>();
    private boolean close;
    private TimerConfig timer = new TimerConfig();

    public ClusterRootConfig() {
    }


    @AllArgsConstructor
    @Data
    public static class ClusterConfig {
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

        public ClusterConfig() {
        }

        public List<String>  getAllDatasources(){
            if (masters == null){
                masters = Collections.emptyList();
            }
            if (replicas == null){
                replicas = Collections.emptyList();
            }
            ArrayList<String> nodes = new ArrayList<>(masters.size() + replicas.size());
            nodes.addAll(masters);
            nodes.addAll(replicas);
            return nodes;
        }

    }


    @Data
    public static class HeartbeatConfig {
        private int maxRetry;
        private long minSwitchTimeInterval;
        private long heartbeatTimeout;
        private long slaveThreshold;
        private String reuqestType;
    }

}
