package io.mycat.newquery;

import lombok.Data;

@Data
public class SqlResult {
    long affectRows;
    long lastInsertId;

    public long[] toLongs() {
        return new long[]{affectRows, lastInsertId};
    }

    public static SqlResult of(long affectRows,
                               long lastInsertId) {
        SqlResult sqlResult = new SqlResult();
        sqlResult.affectRows = affectRows;
        sqlResult.lastInsertId = lastInsertId;
        return sqlResult;
    }
}
