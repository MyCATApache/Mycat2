package io.mycat;

import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class LocalLockServiceImpl implements LockService {
   final ConcurrentMap<String, ReentrantLock> map = new ConcurrentHashMap<>();
    @Override
    public Future<Lock> getLockWithTimeout(String name, long timeout) {
        ReentrantLock lock = map.computeIfAbsent(name, s -> new ReentrantLock());
        try {
            if(lock.tryLock(timeout, TimeUnit.MILLISECONDS)){
                return Future.succeededFuture(() -> lock.unlock());
            }else{
              return Future.failedFuture(new MycatException("can not get lock :"+name));
            }
        } catch (InterruptedException e) {
          return   Future.failedFuture(e);
        }

    }
}
