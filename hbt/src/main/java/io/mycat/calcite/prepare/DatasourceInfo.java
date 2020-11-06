package io.mycat.calcite.prepare;

import io.mycat.calcite.table.SingeTargetSQLTable;
import lombok.Getter;

import java.util.List;
import java.util.Map;


@Getter
public class DatasourceInfo {
    final List<SingeTargetSQLTable> preSeq;
    final Map<String, List<SingeTargetSQLTable>> map;

    public DatasourceInfo(List<SingeTargetSQLTable> preSeq, Map<String, List<SingeTargetSQLTable>> map) {
        this.preSeq = preSeq;
        this.map = map;
    }
}