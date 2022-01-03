package io.mycat;

import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public interface LockService {
     Future<Void> lock(String name, Supplier<Future<Void>> runnable);
}
