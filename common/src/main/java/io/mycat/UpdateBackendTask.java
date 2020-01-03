package io.mycat;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@Getter
public class UpdateBackendTask {
    String sql;
    Type type;
    BackendTableInfo backend;
    public enum Type{
        BROADCAST,
        QUERY_UPDATE,
        SINGLE,
    }
}