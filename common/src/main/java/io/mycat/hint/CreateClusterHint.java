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
package io.mycat.hint;

import io.mycat.config.ClusterConfig;
import io.mycat.util.JsonUtil;
import org.jetbrains.annotations.NotNull;

import java.text.MessageFormat;
import java.util.List;

public class CreateClusterHint extends HintBuilder {
    private ClusterConfig config;

    public static String create(String name, List<String> masters, List<String> reads) {
        ClusterConfig clusterConfig = createConfig(name, masters, reads);

        CreateClusterHint createClusterHint = new CreateClusterHint();
        createClusterHint.setConfig(clusterConfig);

        return createClusterHint.build();
    }

    @NotNull
    public static ClusterConfig createConfig(String name, List<String> masters, List<String> reads) {
        ClusterConfig clusterConfig = new ClusterConfig();
        clusterConfig.setName(name);
        clusterConfig.setMasters(masters);
        clusterConfig.setReplicas(reads);
        return clusterConfig;
    }

    public static String create(ClusterConfig clusterConfig) {
        CreateClusterHint createClusterHint = new CreateClusterHint();
        createClusterHint.setConfig(clusterConfig);
        return createClusterHint.build();
    }

    public void setConfig(ClusterConfig config) {
        this.config = config;
    }

    @Override
    public String getCmd() {
        return "createCluster";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(config));
    }
}