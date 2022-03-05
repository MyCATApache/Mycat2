package io.mycat.ratelimiter;

import com.google.common.util.concurrent.RateLimiter;
import io.vertx.core.Future;

import java.util.concurrent.ConcurrentHashMap;

public class RateLimiterService<T> {
    public  final static RateLimiterService<String> STRING_INSTANCE = new RateLimiterService<>();
    ConcurrentHashMap<T, Context> map = new ConcurrentHashMap<>();

    public static void main(String[] args) {

    }

    static class Context {
        final RateLimiter rateLimiter;
        Future<Void> future = Future.succeededFuture();

        public Context(RateLimiter rateLimiter) {
            this.rateLimiter = rateLimiter;
        }
    }

    public void putLimit(T key, int count) {
        Context context1 = map.computeIfAbsent(key, t -> new Context(RateLimiter.create(count)));
        if (context1.rateLimiter.getRate()!=count){
            context1.rateLimiter.setRate(count);
        }
    }

    public void remove(T key){
        map.remove(key);
    }

    public Future<Void> take(T sql) {
        if (map.isEmpty()) {
            return Future.succeededFuture();
        }
        Context context = map.get(sql);
        if (context == null) return Future.succeededFuture();
        if (context.rateLimiter.tryAcquire()) {
            return Future.succeededFuture();
        } else {
            synchronized (context) {
                if (context.future.isComplete()) {
                    context.future = Future.succeededFuture();
                }
                Future<Void> voidFuture = context.future.transform(stringAsyncResult -> take(sql));
                context.future = voidFuture;
                return voidFuture;
            }
        }
    }
}
