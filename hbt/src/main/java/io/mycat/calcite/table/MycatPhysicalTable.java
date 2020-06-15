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
import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.CalciteUtls;
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.calcite.resultset.MyCatResultSetEnumerator;
import io.mycat.statistic.StatisticCenter;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TransientTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * @author Junwen Chen
 **/
@Getter
public class MycatPhysicalTable extends MycatTableBase implements TransientTable, ProjectableFilterableTable, TranslatableTable {
    final MycatLogicTable logicTable;
    final DataNode backendTableInfo;//真实表名
    Statistic statistic;//MycatLogicTable的构造函数没有statistic

    public MycatPhysicalTable(MycatLogicTable logicTable, DataNode backendTableInfo) {
        this.logicTable = logicTable;
        this.backendTableInfo = backendTableInfo;
    }

    @NotNull
    private Statistic createStatistic(Statistic parentStatistic) {
        return new Statistic() {
            @Override
            public Double getRowCount() {
                return StatisticCenter.INSTANCE.getPhysicsTableRow(backendTableInfo.getSchema(),
                        backendTableInfo.getTable(),
                        backendTableInfo.getTargetName());
            }

            @Override
            public boolean isKey(ImmutableBitSet columns) {
                return parentStatistic.isKey(columns);
            }

            @Override
            public List<ImmutableBitSet> getKeys() {
                return parentStatistic.getKeys();
            }

            @Override
            public List<RelReferentialConstraint> getReferentialConstraints() {
                return parentStatistic.getReferentialConstraints();
            }

            @Override
            public List<RelCollation> getCollations() {
                return parentStatistic.getCollations();
            }

            @Override
            public RelDistribution getDistribution() {
                return parentStatistic.getDistribution();
            }
        };
    }

    @Override
    public TableHandler logicTable() {
        return logicTable.logicTable();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        String backendTaskSQL = CalciteUtls.getBackendTaskSQL(filters,
                logicTable().getColumns(),
                CalciteUtls.getColumnList(logicTable(), projects), backendTableInfo);

        MycatCalciteDataContext root1 = (MycatCalciteDataContext) root;
        MycatConnection connection = root1.getUponDBContext().getConnection(backendTableInfo.getTargetName());
        RowBaseIterator rowBaseIterator = connection.executeQuery(null, backendTaskSQL);
        return new AbstractEnumerable<Object[]>() {
            @Override
            @SneakyThrows
            public Enumerator<Object[]> enumerator() {
                return new MyCatResultSetEnumerator(root1.getCancelFlag(), rowBaseIterator);
            }
        };

    }

    public String getTargetName() {
        return backendTableInfo.getTargetName();
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return LogicalTableScan.create(context.getCluster(), relOptTable, ImmutableList.of());
    }

    @Override
    public Statistic getStatistic() {
        if (statistic == null) {
            Statistic parentStatistic = logicTable.getStatistic();
            statistic = createStatistic(parentStatistic);
        }
        return this.statistic;
    }
}
