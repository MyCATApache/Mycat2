package io.mycat;

import io.mycat.util.VertxUtil;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.spi.metrics.PoolMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

public class IOExecutor {
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final VertxMetrics metrics;
    private PoolMetrics<?> poolMetrics;

    public IOExecutor(VertxMetrics metrics) {
        this.metrics = metrics;
        this.poolMetrics = metrics.createPoolMetrics("io", "mycat.io-thread", Integer.MAX_VALUE);
    }

    public <T> Future<T> executeBlocking(Handler<Promise<T>> blockingCodeHandler) {
        Promise<T> promise = VertxUtil.newPromise();
        PoolMetrics metrics = poolMetrics;
        Object queueMetric = metrics != null ? metrics.submitted() : null;
        Future<T> fut = promise.future();
        try {
            Runnable command = () -> {
                Object execMetric = null;
                if (metrics != null) {
                    execMetric = metrics.begin(queueMetric);
                }
                try {
                    blockingCodeHandler.handle(promise);
                } catch (Throwable e) {
                    promise.tryFail(e);
                }
                if (metrics != null) {
                    metrics.end(execMetric, fut.succeeded());
                }
            };
            executorService.execute(command);
        } catch (RejectedExecutionException e) {
            // Pool is already shut down
            if (metrics != null) {
                metrics.rejected(queueMetric);
            }
            throw e;
        }
        return fut;
    }
}
