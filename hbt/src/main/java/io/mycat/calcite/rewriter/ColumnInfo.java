package io.mycat.calcite.rewriter;

import lombok.Getter;
import org.apache.calcite.rel.core.TableScan;

@Getter
public class ColumnInfo {
    private TableScan tableScan;
    private int index;

    public ColumnInfo(TableScan tableScan, int index) {
        this.tableScan = tableScan;
        this.index = index;
    }
}