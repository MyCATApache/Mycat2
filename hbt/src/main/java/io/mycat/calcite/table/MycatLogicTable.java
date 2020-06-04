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
import io.mycat.BackendTableInfo;
import io.mycat.metadata.GlobalTableHandler;
import io.mycat.metadata.LogicTableType;
import io.mycat.metadata.ShardingTableHandler;
import io.mycat.metadata.TableHandler;
import io.mycat.queryCondition.SimpleColumnInfo;
import io.mycat.statistic.StatisticCenter;
import lombok.Getter;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Junwen Chen
 **/
@Getter
public class MycatLogicTable extends MycatTableBase implements TranslatableTable {
    final TableHandler table;
    final List<MycatPhysicalTable> dataNodes = new ArrayList<>();
    final Map<String, MycatPhysicalTable> dataNodeMap = new HashMap<>();
    final Statistic statistic;
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatLogicTable.class);

    public MycatLogicTable(TableHandler t) {
        this.table = t;

        switch (table.getType()) {
            case SHARDING: {
                ShardingTableHandler table = (ShardingTableHandler) t;
                for (BackendTableInfo backend : table.getShardingBackends()) {
                    MycatPhysicalTable mycatPhysicalTable = new MycatPhysicalTable(this, backend);
                    dataNodes.add(mycatPhysicalTable);
                    dataNodeMap.put(backend.getUniqueName(), mycatPhysicalTable);
                }
                ImmutableList.Builder<ImmutableBitSet> indexes = ImmutableList.builder();
                try {
                    SimpleColumnInfo replicaColumnInfo = Optional.ofNullable(table.getReplicaColumnInfo())
                            .map(i -> i.getColumnInfo()).orElse(null);
                    SimpleColumnInfo databaseColumnInfo = Optional.ofNullable(table.getDatabaseColumnInfo())
                            .map(i -> i.getColumnInfo()).orElse(null);
                    SimpleColumnInfo tableColumnInfo = Optional.ofNullable(table.getTableColumnInfo())
                            .map(i -> i.getColumnInfo()).orElse(null);
                    SimpleColumnInfo natureTableColumnInfo = Optional.ofNullable(table.getNatureTableColumnInfo())
                            .map(i -> i.getColumnInfo()).orElse(null);

                    int index = 0;

                    for (SimpleColumnInfo column : table.getColumns()) {
                        boolean isIndex = false;
                        if (column.equals(replicaColumnInfo)) {
                            isIndex = true;
                        }
                        if (column.equals(databaseColumnInfo)) {
                            isIndex = true;
                        }
                        if (column.equals(tableColumnInfo)) {
                            isIndex = true;
                        }
                        if (column.equals(natureTableColumnInfo)) {
                            isIndex = true;
                        }
                        if (column.isPrimaryKey()) {
                            isIndex = true;
                        }
                        if (column.isIndex()) {
                            isIndex = true;
                        }
                        if (isIndex) {
                            indexes.add(ImmutableBitSet.of(index));
                        }
                        index++;
                    }

                    if (!table.isNatureTable()) {
                        List<SimpleColumnInfo> columns = table.getColumns();
                        int replicaColumnInfoIndex = columns.indexOf(table.getReplicaColumnInfo().getColumnInfo());
                        int databaseColumnInfoIndex = columns.indexOf(table.getDatabaseColumnInfo().getColumnInfo());
                        int tableColumnInfoIndex = columns.indexOf(table.getTableColumnInfo().getColumnInfo());


                        indexes.addAll(Arrays.asList(
                                ImmutableBitSet.of(replicaColumnInfoIndex, databaseColumnInfoIndex),
                                ImmutableBitSet.of(replicaColumnInfoIndex, tableColumnInfoIndex),
                                ImmutableBitSet.of(databaseColumnInfoIndex, tableColumnInfoIndex)
                        ));

                    }


                } catch (Throwable e) {
                    LOGGER.error("", e);
                }
                ImmutableList<ImmutableBitSet> immutableBitSets = indexes.build();
                statistic = createStatistic(immutableBitSets);
                break;
        }
        case GLOBAL: {
            GlobalTableHandler table = (GlobalTableHandler) t;
            for (Map.Entry<String, BackendTableInfo> stringBackendTableInfoEntry : table.getDataNodeMap().entrySet()) {
                MycatPhysicalTable mycatPhysicalTable = new MycatPhysicalTable(this, stringBackendTableInfoEntry.getValue());
                dataNodes.add(mycatPhysicalTable);
                dataNodeMap.put(stringBackendTableInfoEntry.getValue().getUniqueName(), mycatPhysicalTable);
            }
            ImmutableList.Builder<ImmutableBitSet> builder = ImmutableList.builder();

            try {
                int index = 0;
                for (SimpleColumnInfo column : table.getColumns()) {
                    if (column.isIndex() || column.isPrimaryKey()) {
                        builder.add(ImmutableBitSet.of(index));
                    }
                    index++;
                }
            }catch (Throwable e){
                LOGGER.error("",e);
            }

            ImmutableList<ImmutableBitSet> build = builder.build();
            statistic = createStatistic(build);
            break;
        }
        default:
        statistic = Statistics.UNKNOWN;
    }
}

    private Statistic createStatistic(ImmutableList<ImmutableBitSet> immutableBitSets) {
      return new Statistic() {
            public Double getRowCount() {
                return StatisticCenter.INSTANCE.getLogicTableRow(table.getSchemaName(),table.getTableName());
            }

            public boolean isKey(ImmutableBitSet columns) {
                return immutableBitSets.contains(columns);
            }

            public List<ImmutableBitSet> getKeys() {
                return immutableBitSets;
            }

            public List<RelReferentialConstraint> getReferentialConstraints() {
                return ImmutableList.of();
            }

            public List<RelCollation> getCollations() {
                return ImmutableList.of();
            }

            public RelDistribution getDistribution() {
                return RelDistributionTraitDef.INSTANCE.getDefault();
            }
        };
    }

    public MycatPhysicalTable getMycatPhysicalTable(String uniqueName) {
        return dataNodeMap.get(uniqueName);
    }


    @Override
    public TableHandler logicTable() {
        return table;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return LogicalTableScan.create(context.getCluster(), relOptTable);
    }

    public MycatPhysicalTable getMycatGlobalPhysicalTable(Set<String> context) {
        if (table.getType() != LogicTableType.GLOBAL) {
            throw new AssertionError();
        }
        BackendTableInfo globalBackendTableInfo = ((GlobalTableHandler) table).getMycatGlobalPhysicalBackendTableInfo(context);
        return new MycatPhysicalTable(this, globalBackendTableInfo);
    }

    @Override
    public Statistic getStatistic() {
        return statistic;
    }
}