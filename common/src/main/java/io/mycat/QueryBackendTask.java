package io.mycat;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@ToString
@Getter
public class QueryBackendTask {
    String sql;
    BackendTableInfo backendTableInfo;
}