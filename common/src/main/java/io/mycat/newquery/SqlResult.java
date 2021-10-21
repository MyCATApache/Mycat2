package io.mycat.newquery;

import lombok.Data;

@Data
public class SqlResult {
    long affectRows;
    long lastInsertId;

    public long[] toLongs(){
        return new long[]{affectRows,lastInsertId};
    }
}
