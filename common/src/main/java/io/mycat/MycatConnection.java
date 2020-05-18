package io.mycat;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.beans.mycat.MycatRowMetaData;

public interface MycatConnection extends AutoCloseable,Wrapper {

    public UpdateRowIteratorResponse executeUpdate(String sql, boolean needGeneratedKeys, int serverStatus);

    public void close();

    public boolean isClosed();

    public RowBaseIterator executeQuery(MycatRowMetaData mycatRowMetaData, String sql);
}