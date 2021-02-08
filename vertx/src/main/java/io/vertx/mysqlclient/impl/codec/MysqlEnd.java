package io.vertx.mysqlclient.impl.codec;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class MysqlEnd implements MysqlPacket {
    private int sequenceId;
    private int serverStatusFlags;
    private long affectedRows;
    private long lastInsertId;

    @Override
    public byte[] toBytes() {
        return new byte[0];
    }
}
