package io.mycat.api.collector;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class MysqlRow implements MysqlPayloadObject {
    private Object[] row;
}
