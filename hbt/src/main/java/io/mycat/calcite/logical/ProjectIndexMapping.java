package io.mycat.calcite.logical;

import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.util.List;

@Getter
public class ProjectIndexMapping {
    private List<Integer> indexColumns;
    private List<Integer> factColumns;
    private List<Integer> fixProjects;
    public ProjectIndexMapping(List<Integer> indexColumns,
                               List<Integer> factColumns,
                               List<Integer> fixProjects) {
        this.indexColumns = indexColumns;
        this.factColumns = factColumns;
        this.fixProjects = fixProjects;
    }
}
