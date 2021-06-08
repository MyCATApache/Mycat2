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
package io.mycat.calcite.table;

import com.google.common.collect.ImmutableList;
import io.mycat.Partition;
import io.mycat.TableHandler;
import io.mycat.calcite.rewriter.Distribution;
import lombok.Getter;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;

/**
 * @author Junwen Chen
 **/
@Getter
public class MycatPhysicalTable extends MycatTableBase implements AbstractMycatTable, TranslatableTable {
    final MycatLogicTable logicTable;
    final Partition partition;//真实表名
    final Statistic statistic;//MycatLogicTable的构造函数没有statistic

    public MycatPhysicalTable(MycatLogicTable logicTable, Partition partition) {
        this.logicTable = logicTable;
        this.partition = partition;
        this.statistic = Statistics.createStatistic(logicTable.getStatistic(), partition);
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
    public Distribution createDistribution() {
        throw new UnsupportedOperationException();
    }
    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return LogicalTableScan.create(context.getCluster(),relOptTable,ImmutableList.of());
    }
}
