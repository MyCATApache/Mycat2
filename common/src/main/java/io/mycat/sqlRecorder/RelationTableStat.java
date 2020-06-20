package io.mycat.sqlRecorder;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class RelationTableStat {
    private final String namespace;
    private final long count;
}