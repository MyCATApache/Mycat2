package io.mycat.calcite.logic;

import io.mycat.QueryBackendTask;
import io.mycat.calcite.CalciteUtls;
import io.mycat.calcite.MetadataManager;
import io.mycat.calcite.MyCatResultSetEnumerable;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rex.RexNode;

import java.util.List;

import static io.mycat.calcite.CalciteUtls.getQueryBackendTasks;

public class MycatLogicTable extends MycatTableBase {
    final MetadataManager.LogicTable table;

    public MycatLogicTable(MetadataManager.LogicTable table) {
        this.table = table;
    }

    @Override
    public MetadataManager.LogicTable logicTable() {
        return table;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        List<QueryBackendTask> backendTasks = getQueryBackendTasks(this.table,  filters, projects);
        return new MyCatResultSetEnumerable(CalciteUtls.getCancelFlag(root), backendTasks);
    }
}