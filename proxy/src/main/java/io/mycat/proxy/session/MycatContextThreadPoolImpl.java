package io.mycat.proxy.session;

import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.ScheduleUtil;
import io.mycat.bindthread.BindThreadCallback;
import io.mycat.config.ThreadPoolExecutorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class MycatContextThreadPoolImpl implements MycatContextThreadPool {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatContextThreadPoolImpl.class);
    private final ExecutorService noBindingPool;
    private final Consumer<Exception> exceptionHandler;
    final long waitTaskTimeout;
    final TimeUnit timeoutUnit;

    public MycatContextThreadPoolImpl(ThreadPoolExecutorConfig worker, ExecutorService noBindingPool) {
        this(noBindingPool, (e) -> LOGGER.error("", e), worker.getTaskTimeout(), TimeUnit.valueOf(worker.getTimeUnit()));
    }

    public MycatContextThreadPoolImpl(ExecutorService noBindingPool,
                                      Consumer<Exception> exceptionHandler,
                                      long waitTaskTimeout,
                                      TimeUnit timeoutUnit) {
        this.noBindingPool = noBindingPool;
        this.exceptionHandler = exceptionHandler;
        this.waitTaskTimeout = waitTaskTimeout;
        this.timeoutUnit = timeoutUnit;
    }

    @Override
    public void run(MycatDataContext key, BindThreadCallback task) {
        ScheduleUtil.TimerTask timerFuture = ScheduleUtil.getTimerFuture(new Closeable() {
            @Override
            public void close() throws IOException {
                task.onException(key, new MycatException("task timeout"));
            }
        }, waitTaskTimeout, timeoutUnit);
        Future<?> submit = noBindingPool.submit(() -> {
            try {
                task.accept(key, null);
            } catch (Exception e) {
                task.onException(key, e);
                exceptionHandler.accept(e);
            } finally {
                timerFuture.setFinished();
                task.finallyAccept(key, null);

            }
        });
    }


    @Override
    public void runOnBinding(MycatDataContext container, BindThreadCallback bindThreadCallback) {
        throw new IllegalArgumentException();
    }
}