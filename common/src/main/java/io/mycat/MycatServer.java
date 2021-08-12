package io.mycat;

import io.mycat.api.collector.RowBaseIterator;

import java.util.List;

public interface MycatServer {

    RowBaseIterator showNativeDataSources();

    RowBaseIterator showConnections();

    RowBaseIterator showReactors();

    RowBaseIterator showBufferUsage(long sessionId);

    RowBaseIterator showNativeBackends();

    long countConnection();

    void start() throws Exception;

    int kill(List<Long> id);
}
