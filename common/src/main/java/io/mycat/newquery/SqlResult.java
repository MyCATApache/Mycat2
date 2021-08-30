package io.mycat.newquery;

import lombok.Data;

@Data
public class SqlResult {
    long affectRows;
    long lastInsertId;
}
