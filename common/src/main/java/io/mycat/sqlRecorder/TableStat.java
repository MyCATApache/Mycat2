package io.mycat.sqlRecorder;

import lombok.Builder;

import java.util.ArrayList;
import java.util.List;

@Builder
public class TableStat {
    private final String namespace;
    private final long rCount = 0;
    private final long wCount = 0;
    private final List<RelationTableStat> relationTableStats = new ArrayList<>(0);
}