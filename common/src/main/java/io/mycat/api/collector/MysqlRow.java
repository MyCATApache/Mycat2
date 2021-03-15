package io.mycat.api.collector;

import lombok.Getter;

@Getter
public class MysqlRow implements MysqlPayloadObject {
    private Object[] row;

    public MysqlRow(Object[] row) {
        this.row = row;
    }
}
