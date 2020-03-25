/**
 * Copyright (C) <2020>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat;

import io.mycat.config.*;
import io.mycat.util.YamlUtil;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class MycatConfig {
    PatternRootConfig interceptor = new PatternRootConfig();
    ShardingQueryRootConfig metadata = new ShardingQueryRootConfig();
    DatasourceRootConfig datasource = new DatasourceRootConfig();
    ClusterRootConfig cluster = new ClusterRootConfig();
    //    SecurityConfig security = new SecurityConfig();
    PlugRootConfig plug = new PlugRootConfig();
    ServerConfig server = new ServerConfig();
    List<String> packageNameList = new ArrayList<>();
    Map<String,String> properties;
    boolean debug;
    public static void main(String[] args) {
        MycatConfig mycatConfig = new MycatConfig();
        String dump = YamlUtil.dump(mycatConfig);
        System.out.println(dump);
    }
}