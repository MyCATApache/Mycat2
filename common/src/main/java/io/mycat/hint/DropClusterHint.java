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

import java.text.MessageFormat;

public class DropClusterHint extends HintBuilder {
    private ClusterConfig config;

    public static String create(String name) {
        ClusterConfig clusterConfig = new ClusterConfig();
        clusterConfig.setName(name);
        DropClusterHint dropClusterHint = new DropClusterHint();
        dropClusterHint.setConfig(clusterConfig);

        return dropClusterHint.build();
    }

    public static DropClusterHint create(ClusterConfig clusterConfig) {
        DropClusterHint dropClusterHint = new DropClusterHint();
        dropClusterHint.setConfig(clusterConfig);
        return dropClusterHint;
    }

    public void setConfig(ClusterConfig config) {
        this.config = config;
    }

    @Override
    public String getCmd() {
        return "dropCluster";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*! mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(config));
    }
}