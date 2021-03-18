package io.mycat.calcite.rewriter;

import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.rel.core.TableScan;

import java.util.Objects;

@Getter
@ToString
public class ColumnInfo {
    private TableScan tableScan;
    private int index;

    public ColumnInfo(TableScan tableScan, int index) {
        this.tableScan = tableScan;
        this.index = index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnInfo that = (ColumnInfo) o;
        return index == that.index && Objects.equals(tableScan, that.tableScan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableScan, index);
    }
}