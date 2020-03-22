package io.mycat.calcite.prepare;

import io.mycat.calcite.table.PreComputationSQLTable;
import lombok.Getter;

import java.util.List;
import java.util.Map;


@Getter
public class DatasourceInfo {
    final List<PreComputationSQLTable> preSeq;
    final Map<String, List<PreComputationSQLTable>> map;

    public DatasourceInfo(List<PreComputationSQLTable> preSeq, Map<String, List<PreComputationSQLTable>> map) {
        this.preSeq = preSeq;
        this.map = map;
    }
}