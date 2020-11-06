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
import io.mycat.RangeVariable;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author cjw
 * 自定义路由算法接口
 */
public abstract class CustomRuleFunction {
    protected Map<String, Object> properties;
    protected Map<String, Object> ranges;
    protected ShardingTableHandler table;

    public abstract String name();

    public abstract List<DataNode> calculate(Map<String, Collection<RangeVariable>> values);

    protected abstract void init(ShardingTableHandler tableHandler, Map<String, Object> properties, Map<String, Object> ranges);

    public Map<String, Object> getProperties() {
        return properties;
    }

    public Map<String, Object> getRanges() {
        return ranges;
    }

    public synchronized void callInit(ShardingTableHandler tableHandler, Map<String, Object> properties, Map<String, Object> ranges) {
        this.properties = properties;
        this.ranges = ranges;
        this.table = tableHandler;
        init(table, properties, ranges);
    }

    public ShardingTableHandler getTable() {
        return table;
    }



    public boolean isSameRule(CustomRuleFunction other) {
        return false;
    }
 public abstract    boolean isShardingKey(String name);
}