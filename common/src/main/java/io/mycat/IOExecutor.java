package io.mycat;

import io.mycat.util.VertxUtil;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxImpl;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.core.spi.metrics.PoolMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

public interface IOExecutor {

    public <T> Future<T> executeBlocking(Handler<Promise<T>> blockingCodeHandler);

    long count();

    public static final IOExecutor DEFAULT = new IOExecutor() {
        final ExecutorService executorService = Executors.newCachedThreadPool();
        private final AtomicLong count = new AtomicLong();
        public <T> Future<T> executeBlocking(Handler<Promise<T>> blockingCodeHandler) {
            count.getAndIncrement();
            PromiseInternal<Object> promise = VertxUtil.newPromise();
            try {
                executorService.execute(() -> {
                    try {
                        blockingCodeHandler.handle((Promise<T>) promise);
                    }catch (Exception e){
                        promise.tryFail(e);
                    }
                });
                return (Future) promise.future();
            }finally {
                count.decrementAndGet();
            }
        }

        @Override
        public long count() {
            return count.get();
        }
    };

    public static IOExecutor fromVertx(Vertx vertx) {
        class VertxIOExecutor implements IOExecutor {
            private final ExecutorService executorService = Executors.newCachedThreadPool();
            private final VertxMetrics metrics;
            private PoolMetrics<?> poolMetrics;
            private final AtomicLong count = new AtomicLong();

            public VertxIOExecutor(VertxMetrics metrics) {
                this.metrics = metrics;
                this.poolMetrics = metrics.createPoolMetrics("io", "mycat.io-thread", Integer.MAX_VALUE);
            }

            public <T> Future<T> executeBlocking(Handler<Promise<T>> blockingCodeHandler) {
                count.getAndIncrement();
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
                }finally {
                    count.decrementAndGet();
                }
                return fut;
            }

            @Override
            public long count() {
                return count.get();
            }
        }
        return new VertxIOExecutor((VertxMetrics) ((VertxImpl) vertx).getMetrics());
    }

}
