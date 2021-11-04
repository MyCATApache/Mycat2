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

package io.mycat;

import lombok.Getter;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;


@Getter
public enum ShardingTableType {
    SHARDING_INSTANCE_SINGLE_TABLE(true),
    SINGLE_INSTANCE_SHARDING_TABLE(false),
    SHARDING_INSTANCE_SHARDING_TABLE(false),
    ;

    private boolean singleTablePerInstance;

    ShardingTableType(boolean singleTablePerInstance) {
        this.singleTablePerInstance = singleTablePerInstance;
    }

    public static ShardingTableType DEFAULT = ShardingTableType.SHARDING_INSTANCE_SHARDING_TABLE;

    public static ShardingTableType computeByName(Collection<Partition> partitions) {
        if (partitions == null || partitions.isEmpty()) {
            return SHARDING_INSTANCE_SHARDING_TABLE;
        }
        if (partitions.stream().map(i -> i.getTargetName()).distinct().count() == 1) {
            return SINGLE_INSTANCE_SHARDING_TABLE;
        }
        Map<String, Set<String>> collect = partitions.stream().collect(Collectors.groupingBy(k -> k.getTargetName(), Collectors.mapping(i -> i.getSchema() + i.getTable(), Collectors.toSet())));
        if (collect.entrySet().stream().allMatch(i -> i.getValue().size() == 1)) {
            return SHARDING_INSTANCE_SINGLE_TABLE;
        }
        return SHARDING_INSTANCE_SHARDING_TABLE;
    }

    /**
     * when it ensures only a target(allow partitions),when right only a table
     *
     * @param right
     * @return
     */
    public boolean joinSameTarget(ShardingTableType right) {
        return right.isSingleTablePerInstance();
    }
}