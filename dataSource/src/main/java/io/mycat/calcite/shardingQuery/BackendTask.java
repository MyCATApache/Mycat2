package io.mycat.calcite.shardingQuery;

import io.mycat.calcite.BackendTableInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@ToString
@Getter
public class BackendTask {
    String sql;
    boolean update;
    BackendTableInfo backendTableInfo;
}