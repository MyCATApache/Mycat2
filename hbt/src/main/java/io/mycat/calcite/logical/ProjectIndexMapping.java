package io.mycat.calcite.logical;

import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.util.List;

@Getter
public class ProjectIndexMapping {
    private List<String> indexColumns;
    private List<String> factColumns;
    public ProjectIndexMapping(List<String> indexColumns,
                               List<String> factColumns) {
        this.indexColumns = indexColumns;
        this.factColumns = factColumns;
    }

   public boolean needFactTable(){
       return !indexColumns.containsAll(factColumns);
    }
}
