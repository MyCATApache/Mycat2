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

import java.util.List;
import java.util.Map;

public interface ConfigProvider {
    /**
     * 配置提供者
     * @param rootClass 加载主类,根据此类找到对应的resource文件夹
     * @param config 初始化参数
     * @throws Exception
     */
    void init(Class rootClass, Map<String,String> config) throws Exception;
    void fetchConfig(String path) throws Exception;
    void fetchConfig() throws Exception;
    void report(MycatConfig changed);

    public MycatConfig currentConfig();

    public Map<String, Object>  globalVariables();

    void reportReplica(String replicaName, List<String> dataSourceList);
}