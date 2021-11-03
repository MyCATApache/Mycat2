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

import java.util.Collection;

public enum ShardingTableType {
    SHARDING_INSTANCE_SINGLE_TABLE,
    SINGLE_INSTANCE_SHARDING_TABLE,
    SHARDING_INSTANCE_SHARDING_TABLE,
    ;
    public static ShardingTableType compute(Collection<Partition> partitions){
        ShardingTableType type = SHARDING_INSTANCE_SHARDING_TABLE;
        if (partitions.stream().map(i -> i.getTargetName()).distinct().count()==1){
            return SINGLE_INSTANCE_SHARDING_TABLE;
        }
        if(partitions.stream().map(i -> i.getSchema() + i.getTable()).distinct().count()==1){
            return SHARDING_INSTANCE_SINGLE_TABLE;
        }
        return type;
    }
}