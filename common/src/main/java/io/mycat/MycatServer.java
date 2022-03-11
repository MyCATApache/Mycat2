package io.mycat;

import io.mycat.api.collector.RowBaseIterator;
import io.vertx.core.Future;

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

    void stopAcceptConnect();

    void resumeAcceptConnect();

    void setReadyToCloseSQL(String sql);

    public Future<Void> pause(long currentId);

    void resume();

    boolean isPause();
}
