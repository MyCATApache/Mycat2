/**
 * Copyright (C) <2019>  <chen junwen,gaozhiwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


package io.mycat.config.datasource;

import io.mycat.config.ConfigurableRoot;
import java.util.List;

/**
 * Desc: 用于加载datasource.yml的类
 *
 * date: 10/09/2017
 * @author: gaozhiwen
 */
public class ReplicasRootConfig implements ConfigurableRoot {
    private List<ReplicaConfig> replicas;
    private String charset;

    public String getCharset() {
        return charset == null?"uft8mb4":charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public List<ReplicaConfig> getReplicas() {
        return replicas;
    }

    public void setReplicas(List<ReplicaConfig> replicas) {
        this.replicas = replicas;
    }
}
