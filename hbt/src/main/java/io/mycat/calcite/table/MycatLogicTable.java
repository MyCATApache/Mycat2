/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.calcite.table;

import com.google.common.collect.ImmutableList;
import io.mycat.DataNode;
import io.mycat.TableHandler;
import io.mycat.calcite.CalciteUtls;
import io.mycat.hbt3.AbstractMycatTable;
import io.mycat.hbt3.Distribution;
import io.mycat.hbt4.ShardingInfo;
import io.mycat.metadata.GlobalTableHandler;
import io.mycat.metadata.NormalTableHandler;
import io.mycat.router.ShardingTableHandler;
import lombok.Getter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Junwen Chen
 **/
@Getter
public class MycatLogicTable extends MycatTableBase implements AbstractMycatTable {
    final TableHandler table;
    final Statistic statistic;
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatLogicTable.class);
    private final ShardingInfo shardingInfo;

    public MycatLogicTable(TableHandler t) {
        this.table = t;
        this.statistic = Statistics.createStatistic(table.getSchemaName(), table.getTableName(), table.getColumns());
        this.shardingInfo = ShardingInfo.create(t);
    }

    @Override
    public TableHandler logicTable() {
        return table;
    }

    @Override
    public Statistic getStatistic() {
        return statistic;
    }


    @Override
    public Distribution computeDataNode(List<RexNode> conditions) {
        switch (table.getType()) {
            case SHARDING:
                ShardingTableHandler shardingTableHandler = (ShardingTableHandler) this.table;
                List<DataNode> backendTableInfos = CalciteUtls.getBackendTableInfos(shardingTableHandler, conditions);
                return Distribution.of(backendTableInfos, shardingTableHandler.function().name(), Distribution.Type.Sharding);
            case GLOBAL:
                return computeDataNode();
            case NORMAL:
                return computeDataNode();
        }
        throw new UnsupportedOperationException();
    }

    public Distribution computeDataNode() {
        switch (table.getType()) {
            case SHARDING:
                ShardingTableHandler shardingTableHandler = (ShardingTableHandler) this.table;
                return Distribution.of(shardingTableHandler.dataNodes(), shardingTableHandler.function().name(), Distribution.Type.Sharding);
            case GLOBAL:
                GlobalTableHandler globalTableHandler = (GlobalTableHandler) this.table;
                List<DataNode> globalDataNode = globalTableHandler.getGlobalDataNode();
                int i = ThreadLocalRandom.current().nextInt(0, globalDataNode.size());
                return Distribution.of(ImmutableList.of(globalDataNode.get(i)), getShardingInfo().getDigest(), Distribution.Type.BroadCast);
            case NORMAL:
                DataNode dataNode = ((NormalTableHandler) table).getDataNode();
                return Distribution.of(ImmutableList.of(dataNode), getShardingInfo().getDigest(), Distribution.Type.PHY);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public ShardingInfo getShardingInfo() {
        return shardingInfo;
    }

}