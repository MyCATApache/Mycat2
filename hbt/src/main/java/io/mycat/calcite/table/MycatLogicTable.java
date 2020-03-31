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

import io.mycat.BackendTableInfo;
import io.mycat.QueryBackendTask;
import io.mycat.calcite.CalciteConvertors;
import io.mycat.calcite.CalciteUtls;
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.resultset.MyCatResultSetEnumerable;
import io.mycat.metadata.GlobalTableHandler;
import io.mycat.metadata.LogicTableType;
import io.mycat.metadata.ShardingTableHandler;
import io.mycat.metadata.TableHandler;
import lombok.Getter;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.TranslatableTable;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.mycat.calcite.CalciteUtls.getColumnList;
import static io.mycat.calcite.CalciteUtls.getQueryBackendTasks;

/**
 * @author Junwen Chen
 **/
@Getter
public class MycatLogicTable extends MycatTableBase implements TranslatableTable {
    final TableHandler table;
    final List<MycatPhysicalTable> dataNodes = new ArrayList<>();
    final Map<String, MycatPhysicalTable> dataNodeMap = new HashMap<>();

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
                break;
            }
            case GLOBAL: {
                GlobalTableHandler table = (GlobalTableHandler) t;
                for (Map.Entry<String, BackendTableInfo> stringBackendTableInfoEntry : table.getDataNodeMap().entrySet()) {
                    MycatPhysicalTable mycatPhysicalTable = new MycatPhysicalTable(this, stringBackendTableInfoEntry.getValue());
                    dataNodes.add(mycatPhysicalTable);
                    dataNodeMap.put(stringBackendTableInfoEntry.getValue().getUniqueName(), mycatPhysicalTable);
                }
                break;
            }
            case ER: {
                break;
            }
        }

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
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);

        if (root instanceof io.mycat.calcite.MycatCalciteDataContext) {
            MycatCalciteDataContext root1 = (MycatCalciteDataContext) root;
            MyCatResultSetEnumerable.GetRow getRow = (mycatRowMetaData, targetName, sql) -> {
                return root1.getUponDBContext().query(mycatRowMetaData, targetName, sql);
            };
            RelDataType rowType = CalciteConvertors.getRelDataType(getColumnList(table, projects), MycatCalciteSupport.INSTANCE.TypeFactory);
            if (rowType.getFieldNames().isEmpty()) {
                rowType = getRowType();
            }
            switch (table.getType()) {
                case SHARDING:
                    List<QueryBackendTask> backendTasks = getQueryBackendTasks((ShardingTableHandler) this.table, filters, projects);
                    return new MyCatResultSetEnumerable(getRow, cancelFlag, rowType, backendTasks);
                case GLOBAL:
                    GlobalTableHandler table = (GlobalTableHandler) this.table;
                    BackendTableInfo globalBackendTableInfo = table.getGlobalBackendTableInfoForQuery(root1.getUponDBContext().isInTransaction());
                    String backendTaskSQL = CalciteUtls.getBackendTaskSQL(filters,
                            table.getColumns(),
                            getColumnList(table, projects)
                            , globalBackendTableInfo);
                    return new MyCatResultSetEnumerable((mycatRowMetaData, targetName, sql) -> root1.getUponDBContext().query(mycatRowMetaData, targetName, sql), cancelFlag, rowType, new QueryBackendTask(globalBackendTableInfo.getTargetName(), backendTaskSQL));
                case ER:
                default:
                    throw new UnsupportedOperationException();
            }
        }
        throw new UnsupportedOperationException("不支持的关系表达式 "+filters);
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
}