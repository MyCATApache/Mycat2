package io.mycat;

import io.mycat.api.collector.RowBaseIterator;

public interface MycatServer {

    RowBaseIterator showNativeDataSources();

    RowBaseIterator showConnections();

    RowBaseIterator showReactors();

    RowBaseIterator showBufferUsage(long sessionId);

    RowBaseIterator showNativeBackends();

    void start() throws Exception;
}
