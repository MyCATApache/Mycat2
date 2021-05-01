package io.mycat;

import io.mycat.util.VertxUtil;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.impl.future.PromiseInternal;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IOExecutor {
    final ExecutorService executorService = Executors.newCachedThreadPool();

    public <T> Future<T> executeBlocking(Handler<Promise<T>> blockingCodeHandler) {
        PromiseInternal<Object> promise = VertxUtil.newPromise();
        executorService.execute(() -> blockingCodeHandler.handle((Promise<T>) promise));
        return (Future) promise.future();
    }
}
