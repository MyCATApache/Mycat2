package io.mycat.calcite.logical;

import com.google.common.collect.ImmutableList;
import lombok.Getter;

@Getter
public class IndexMapping {
    private ImmutableList<Integer> indexColumns;
    private ImmutableList<Integer> factColumns;

    public IndexMapping(ImmutableList<Integer> indexColumns, ImmutableList<Integer> factColumns) {
        this.indexColumns = indexColumns;
        this.factColumns = factColumns;
    }
}
