package io.mycat;

import io.vertx.core.Future;

public interface ConfigOps extends AutoCloseable {

    Object currentConfig();

    void commit(Object ops) throws Exception;

    void close();
}