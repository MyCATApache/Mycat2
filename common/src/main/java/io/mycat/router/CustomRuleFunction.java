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
package io.mycat.router;

import com.alibaba.druid.sql.SQLUtils;
import io.mycat.Partition;
import io.mycat.RangeVariable;
import io.mycat.ShardingTableType;

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

    public abstract List<Partition> calculate(Map<String, RangeVariable> values);

    public Partition calculateOne(Map<String, RangeVariable> values) {
        List<Partition> partitions = calculate(values);
        if (partitions.isEmpty()) {
            throw new IllegalArgumentException("路由计算返回结果个数为0");
        }
        if (partitions.size() != 1) {
            List<Partition> dataNodes2 = calculate(values);
             dataNodes2 = calculate(values);
            throw new IllegalArgumentException("路由计算返回结果个数为" + partitions.size());
        }
        Partition partition = partitions.get(0);
        if (partition == null) {
            throw new IllegalArgumentException("路由计算返回结果为NULL");
        }
        return partitions.get(0);
    }

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

    public boolean isShardingKey(String name) {
        name = SQLUtils.normalize(name);
        return isShardingDbKey(name) || isShardingTableKey(name);
    }

    public abstract boolean isShardingDbKey(String name);

    public abstract boolean isShardingTableKey(String name);

    public abstract boolean isShardingTargetKey(String name);

    public boolean isShardingPartitionKey(String name) {
        switch (getShardingTableType()) {
            case SHARDING_INSTANCE_SINGLE_TABLE:
                return isShardingTargetKey(name);
            case SINGLE_INSTANCE_SHARDING_TABLE:
                return isShardingTableKey(name);
            case SHARDING_INSTANCE_SHARDING_TABLE:
                return isShardingTargetKey(name) && isShardingTableKey(name);
            default:
                throw new IllegalStateException("Unexpected value: " + getShardingTableType());
        }
    }

    public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
        return false;
    }

    public abstract String getErUniqueID();

    public abstract ShardingTableType getShardingTableType();
}