package io.mycat;

import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;

import java.util.concurrent.TimeUnit;

public interface LockService {
    default Future<Lock> getLock(String name) {
        return getLock(name, TimeUnit.HOURS.toMillis(2));
    }

    Future<Lock> getLock(String name, long timeout);
}
