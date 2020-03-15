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
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.calcite.resultset.MyCatResultSetEnumerable;
import io.mycat.metadata.LogicTable;
import lombok.Getter;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.TranslatableTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.mycat.calcite.CalciteUtls.getQueryBackendTasks;

/**
 * @author Junwen Chen
 **/
@Getter
public class MycatLogicTable extends MycatTableBase implements TranslatableTable {
    final LogicTable table;
    final List<MycatPhysicalTable> dataNodes;
    final Map<String, MycatPhysicalTable> dataNodeMap = new HashMap<>();

    public MycatLogicTable(LogicTable table) {
        this.table = table;
        this.dataNodes = new ArrayList<>(table.getBackends().size());
        for (BackendTableInfo backend : table.getBackends()) {
            MycatPhysicalTable mycatPhysicalTable = new MycatPhysicalTable(this, backend);
            dataNodes.add(mycatPhysicalTable);
            dataNodeMap.put(backend.getUniqueName(), mycatPhysicalTable);
        }
    }

    public MycatPhysicalTable getMycatPhysicalTable(String uniqueName) {
        return dataNodeMap.get(uniqueName);
    }


    @Override
    public LogicTable logicTable() {
        return table;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        List<QueryBackendTask> backendTasks = getQueryBackendTasks(this.table, filters, projects);
        return new MyCatResultSetEnumerable((MycatCalciteDataContext) root, backendTasks);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return LogicalTableScan.create(context.getCluster(),relOptTable);
    }
}