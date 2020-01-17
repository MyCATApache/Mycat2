package io.mycat.calcite.logic;

import io.mycat.BackendTableInfo;
import io.mycat.calcite.MetadataManager;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class MycatPhysicalTable implements MycatTableBase {
    final MycatLogicTable logicTable;
    final BackendTableInfo backendTableInfo;

    @Override
    public MetadataManager.LogicTable logicTable() {
        return logicTable.logicTable();
    }
}
