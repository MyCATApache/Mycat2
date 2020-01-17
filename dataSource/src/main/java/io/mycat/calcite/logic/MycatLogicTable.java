package io.mycat.calcite.logic;

import io.mycat.calcite.MetadataManager;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

@Getter
public class MycatLogicTable implements MycatTableBase {
    final MetadataManager.LogicTable table;
    final List<MycatPhysicalTable> physicalTables;

    public MycatLogicTable(MetadataManager.LogicTable table) {
        this.table = table;
        this.physicalTables = table.getBackends()
                .stream()
                .map(i -> new MycatPhysicalTable(this, i))
                .collect(Collectors.toList());
    }

    @Override
    public MetadataManager.LogicTable logicTable() {
        return table;
    }
}