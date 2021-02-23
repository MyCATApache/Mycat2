package io.mycat;

import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;

public interface LockService {
    Future<Lock> getLocalLockWithTimeout(String name, long timeout);
}
