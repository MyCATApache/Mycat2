package io.mycat.api.collector;

import io.vertx.sqlclient.Row;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class MysqlRow implements MysqlPayloadObject {
    private Object[] row;
}
