package io.mycat.calcite.logic;

import io.mycat.calcite.MetadataManager;

public class MycatLogicTable implements MycatTableBase {
    final MetadataManager.LogicTable table;

    public MycatLogicTable(MetadataManager.LogicTable table) {
        this.table = table;
    }

    @Override
    public MetadataManager.LogicTable logicTable() {
        return table;
    }
}