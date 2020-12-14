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
import io.mycat.hbt3.AbstractMycatTable;
import io.mycat.hbt3.Distribution;
import io.mycat.hbt4.ShardingInfo;
import lombok.Getter;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;

import java.util.List;

/**
 * @author Junwen Chen
 **/
@Getter
public class MycatPhysicalTable extends MycatTableBase implements AbstractMycatTable, TranslatableTable {
    final MycatLogicTable logicTable;
    final DataNode dataNode;//真实表名
    final Statistic statistic;//MycatLogicTable的构造函数没有statistic

    public MycatPhysicalTable(MycatLogicTable logicTable, DataNode dataNode) {
        this.logicTable = logicTable;
        this.dataNode = dataNode;
        this.statistic = Statistics.createStatistic(logicTable.getStatistic(), dataNode);
    }

    @Override
    public TableHandler logicTable() {
        return logicTable.logicTable();
    }

    @Override
    public Statistic getStatistic() {
        return this.statistic;
    }

    @Override
    public Distribution computeDataNode(List<RexNode> conditions) {
        return Distribution.of(ImmutableList.of(dataNode),false, Distribution.Type.PHY);
    }

    @Override
    public boolean isSingle(List<RexNode> conditions) {
        return true;
    }

    @Override
    public Distribution computeDataNode() {
        return Distribution.of(ImmutableList.of(dataNode),false, Distribution.Type.PHY);
    }

    @Override
    public ShardingInfo getShardingInfo() {
        return logicTable.getShardingInfo();
    }

    @Override
    public boolean isPartial(List<RexNode> conditions) {
        return false;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return LogicalTableScan.create(context.getCluster(),relOptTable,ImmutableList.of());
    }
}
