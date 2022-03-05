package io.mycat.ratelimiter;

import io.vertx.core.Future;

import java.util.HashMap;

public class ComparedRateLimiterServiceImpl<T> extends ComparedRateLimiterService<T> {
    HashMap<T, ContextItem> defaultItemMap = new HashMap<>();

    public ComparedRateLimiterServiceImpl(Context global) {
        super(global);
    }

    @Override
    public Future<Void> take(T key, ContextItem context, Future<Void> endFuture) {
        if (context == null) {
            context = defaultItemMap.get(key);
        }
        return super.take(key, context, endFuture);
    }
}
