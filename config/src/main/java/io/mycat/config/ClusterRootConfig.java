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
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * date: 12/26/2019
 *
 * @author: chenunwen
 */
@Data
public class ClusterRootConfig {
    private List<ClusterConfig> replicas = new ArrayList<>();
    private TimerConfig timer = new TimerConfig();

    @Data
    public class ClusterConfig {
        private String replicaType;
        private String switchType;
        private String readBalanceType;
        private String name;
        private String readBalanceName;
        private String writeBalanceName;
        private long replicaThreshold;
        private List<String> masters = new ArrayList<>();
        private List<String> replicas = new ArrayList<>();
        private HeartbeatConfig heartbeat;
    }


    @Data
    public static class TimerConfig {
        private long initialDelay = 1 * 1000L;
        private long period = 10 * 1000L;
        private String timeUnit = TimeUnit.MILLISECONDS.name();
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
