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
package io.mycat.hbt3;

import io.mycat.hbt4.ShardingInfo;
import org.apache.calcite.rex.RexNode;

import java.util.List;

//@Getter
public interface AbstractMycatTable {
    public abstract Distribution computeDataNode(List<RexNode> conditions);

    public abstract Distribution computeDataNode();

    public abstract ShardingInfo getShardingInfo();

    public default boolean isBroadCast() {
        return getShardingInfo().getType() == ShardingInfo.Type.broadCast;
    }

    public default boolean isNormal() {
        return getShardingInfo().getType() == ShardingInfo.Type.normal;
    }

    public default boolean isSharding() {
        return getShardingInfo().getType() == ShardingInfo.Type.sharding;
    }
}