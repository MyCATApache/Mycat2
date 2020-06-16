/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat.router;

import io.mycat.DataNode;
import io.mycat.TableHandler;

import java.util.List;
import java.util.Map;

/**
 * @author cjw
 * 自定义路由算法接口
 */
public abstract class CustomRuleFunction {
    protected Map<String, String> properties;
    protected Map<String, String> ranges;
    protected ShardingTableHandler table;

    public abstract String name();

    public abstract DataNode calculate(String columnValue);

    public abstract List<DataNode> calculateRange(String beginValue, String endValue);

    protected abstract void init(ShardingTableHandler tableHandler, Map<String, String> properties, Map<String, String> ranges);

    public Map<String, String> getProperties() {
        return properties;
    }

    public Map<String, String> getRanges() {
        return ranges;
    }

    public synchronized void callInit(ShardingTableHandler tableHandler, Map<String, String> properties, Map<String, String> ranges) {
        this.properties = properties;
        this.ranges = ranges;
        this.table = tableHandler;
        init(table, properties, ranges);
    }

    public ShardingTableHandler getTable() {
        return table;
    }
}