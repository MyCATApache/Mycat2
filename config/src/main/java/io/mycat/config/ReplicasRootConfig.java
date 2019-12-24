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

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Desc: 用于加载datasource.yml的类
 * <p>
 * date: 10/09/2017
 *
 * @author: gaozhiwen
 */
@Data
public class ReplicasRootConfig {
    private List<ReplicaConfig> replicas = new ArrayList<>();
    private MasterIndexesConfig masterIndexes = new MasterIndexesConfig();
    private HeartbeatConfig heartbeat = new HeartbeatConfig();

    @Data
    public class ReplicaConfig {
        private String repType;
        private String switchType;
        private String readbalanceType;

        private String name;
        private String readBalanceName;
        private String writeBalanceName;
        private long slaveThreshold;
        private List<String> datasources = new ArrayList<>();
    }

    @Data
    public static class MasterIndexesConfig {
        private Map<String, String> masterIndexes = new HashMap<>();
    }

    @Data
    public static class HeartbeatConfig {
        private long minHeartbeatChecktime = 1 * 1000L;//秒
        private int timerExecutor = 2;
        private long replicaHeartbeatPeriod = 10 * 1000L;
        private long replicaIdleCheckPeriod = 5 * 60 * 1000L;
        private long idleTimeout = 30 * 60 * 1000L;
        private long processorCheckPeriod = 1 * 1000L;
        private long minSwitchTimeInterval = 30 * 60 * 1000L;
        private int maxRetry = 3;
    }

}
