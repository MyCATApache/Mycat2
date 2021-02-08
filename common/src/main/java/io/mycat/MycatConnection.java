package io.mycat;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.mysqlclient.impl.codec.MysqlPacket;

public interface MycatConnection extends AutoCloseable, Wrapper {

    public long[] executeUpdate(String sql, boolean needGeneratedKeys);

    public void close();

    public boolean isClosed();

    public Observable<MysqlPacket> executeQuery(MycatRowMetaData mycatRowMetaData, String sql);
}