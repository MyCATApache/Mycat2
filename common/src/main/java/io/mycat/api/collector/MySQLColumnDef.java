package io.mycat.api.collector;

import io.mycat.beans.mycat.MycatRowMetaData;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class MySQLColumnDef implements MysqlPayloadObject {
    private MycatRowMetaData metaData;
}
