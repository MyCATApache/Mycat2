package io.mycat.connectionschedule;

import io.vertx.core.Future;

public interface CloseFuture {
    Future<Void> close();
}
