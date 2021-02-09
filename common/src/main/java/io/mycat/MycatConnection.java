package io.mycat;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.reactivex.rxjava3.core.Observable;
import io.mycat.api.collector.MysqlPayloadObject;

public interface MycatConnection extends AutoCloseable, Wrapper {

    public long[] executeUpdate(String sql, boolean needGeneratedKeys);

    public void close();

    public boolean isClosed();

    public Observable<MysqlPayloadObject> executeQuery(MycatRowMetaData mycatRowMetaData, String sql);
}