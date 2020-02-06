package io.mycat.calcite.logic;

import io.mycat.BackendTableInfo;
import io.mycat.QueryBackendTask;
import io.mycat.calcite.CalciteUtls;
import io.mycat.calcite.MetadataManager;
import io.mycat.calcite.MyCatResultSetEnumerable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rex.RexNode;

import java.util.List;


@AllArgsConstructor
@Getter
public class MycatPhysicalTable extends MycatTableBase {
    final MycatLogicTable logicTable;
    final BackendTableInfo backendTableInfo;

    @Override
    public MetadataManager.LogicTable logicTable() {
        return logicTable.logicTable();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        String backendTaskSQL = CalciteUtls.getBackendTaskSQL(logicTable(), backendTableInfo, projects, filters);
        return new MyCatResultSetEnumerable(CalciteUtls.getCancelFlag(root), new QueryBackendTask(backendTaskSQL,backendTableInfo.getTargetName()));
    }

    public String getTargetName() {
        return backendTableInfo.getTargetName();
    }
}
