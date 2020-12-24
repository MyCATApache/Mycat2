package io.mycat;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;

public interface MycatConnection extends AutoCloseable, Wrapper {

    public long[] executeUpdate(String sql, boolean needGeneratedKeys);

    public void close();

    public boolean isClosed();

    public RowBaseIterator executeQuery(MycatRowMetaData mycatRowMetaData, String sql);
}