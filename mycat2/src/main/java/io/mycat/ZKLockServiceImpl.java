package io.mycat;

import io.vertx.core.Future;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.shareddata.Lock;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.util.concurrent.TimeUnit;

public class ZKLockServiceImpl implements LockService {
    private final static Logger LOGGER = LoggerFactory.getLogger(LocalLockServiceImpl.class);
    @Override
    public Future<Lock> getLock(String name, long timeout) {
        CuratorFramework curatorFramework = MetaClusterCurrent.wrapper(CuratorFramework.class);
        InterProcessMutex lock = new InterProcessMutex(curatorFramework, "/mycat/lock/"+name);
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
