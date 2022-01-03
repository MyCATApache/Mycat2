package io.mycat;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

public class LocalLockServiceImpl implements LockService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalLockServiceImpl.class);


    @Override
    public synchronized Future<Void> lock(String name, Supplier<Future<Void>> runnable) {
        LOGGER.info("lock_service_worker is running with '" + name + "'");
        try {
            return runnable.get();
        }catch (Throwable throwable){
            return Future.failedFuture(throwable);
        }
    }

}
