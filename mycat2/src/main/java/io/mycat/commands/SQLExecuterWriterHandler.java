package io.mycat.commands;

import io.mycat.beans.resultset.MycatResponse;
import io.vertx.core.impl.future.PromiseInternal;

public interface SQLExecuterWriterHandler {
    public PromiseInternal<Void> writeToMycatSession(MycatResponse response)throws Exception;
}