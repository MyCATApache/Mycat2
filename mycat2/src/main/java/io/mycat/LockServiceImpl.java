package io.mycat;

import io.mycat.config.ZooMap;
import io.vertx.core.Future;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.shareddata.Lock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.util.concurrent.TimeUnit;

public class LockServiceImpl implements LockService {
    private final static Logger LOGGER = LoggerFactory.getLogger(LocalLockServiceImpl.class);
    @Override
    public Future<Lock> getLocalLockWithTimeout(String name, long timeout) {
        InterProcessMutex lock = new InterProcessMutex(ZooMap.getClient(), "mycat/lock/"+name);
        return Future.future(event -> {
            try {
                lock.acquire(timeout, TimeUnit.MILLISECONDS);
                event.complete(() -> {
                    try{
                        lock.release();
                    }catch (Throwable throwable){
                        LOGGER.error(throwable);
                    }
                });
            } catch (Exception e) {
                event.tryFail(e);
            }
        });
    }
}
