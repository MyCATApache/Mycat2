package io.mycat.proxy.handler;

import io.mycat.proxy.session.MycatSession;
import io.vertx.core.impl.future.PromiseInternal;

import java.io.IOException;

/**
 * mycat session写入处理
 */
public interface MycatSessionWriteHandler {

    /**
     * Write to channel.
     *
     * @param session the session
     * @throws IOException the io exception
     */
    PromiseInternal<Void> writeToChannel(MycatSession session) throws IOException;

    /**
     * On exception.
     *
     * @param session the session
     * @param e       the e
     */
    void onException(MycatSession session, Exception e);

    void onClear(MycatSession session);

    WriteType getType();

    public enum WriteType {
        SERVER,
        PROXY
    }
}