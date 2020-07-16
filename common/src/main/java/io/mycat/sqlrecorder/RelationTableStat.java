package io.mycat.sqlrecorder;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class RelationTableStat {
    private final String namespace;
    private final long count;
}