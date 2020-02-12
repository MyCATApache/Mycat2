package io.mycat.calcite.logic;

import io.mycat.BackendTableInfo;
import io.mycat.QueryBackendTask;
import io.mycat.calcite.MyCatResultSetEnumerable;
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.calcite.metadata.MetadataManager;
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

public class MycatLogicTable extends MycatTableBase implements TranslatableTable {
    final MetadataManager.LogicTable table;
    final List<MycatPhysicalTable> dataNodes;
    final Map<String, MycatPhysicalTable> dataNodeMap = new HashMap<>();

    public MycatLogicTable(MetadataManager.LogicTable table) {
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
    public MetadataManager.LogicTable logicTable() {
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