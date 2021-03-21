package io.mycat;

import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;

public interface LockService {
    default Future<Lock> getLockWithTimeout(String name) {
        return getLockWithTimeout(name, 10000);
    }

    Future<Lock> getLockWithTimeout(String name, long timeout);
}
