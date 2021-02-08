package io.vertx.mysqlclient.impl.codec;

import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.sqlclient.impl.command.QueryCommandBase;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class MySQLColumnDef implements MysqlPacket{

    private MySQLRowDesc columnDef;
    private QueryCommandBase queryCommand;


    @Override
    public byte[] toBytes() {
        return new byte[0];
    }
}
