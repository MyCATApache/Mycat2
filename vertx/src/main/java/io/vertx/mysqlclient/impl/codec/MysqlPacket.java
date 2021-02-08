package io.vertx.mysqlclient.impl.codec;

public interface MysqlPacket {

    byte[] toBytes();
}
