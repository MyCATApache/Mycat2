package io.vertx.mysqlclient.impl.codec;

import io.vertx.sqlclient.Row;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class MysqlRow implements MysqlPacket{
    private MySQLColumnDef columnDef;
    private Row row;

    @Override
    public byte[] toBytes() {
        return new byte[0];
    }
}
