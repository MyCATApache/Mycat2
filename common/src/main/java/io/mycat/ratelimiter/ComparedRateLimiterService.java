package io.mycat.ratelimiter;

import com.google.common.util.concurrent.RateLimiter;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.checkerframework.checker.units.qual.C;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

public class ComparedRateLimiterService<T> {
    static class Entry<T> {
        T key;
        ContextItem context;
        Promise<Void> promise;
        Future<Void> endFuture;
    }

    final Context global;
    final ConcurrentHashMap<T, ContextLimit> map = new ConcurrentHashMap<>();
    final ConcurrentLinkedQueue<Entry<T>> queue = new ConcurrentLinkedQueue<>();

    public ComparedRateLimiterService(Context global) {
        this.global = global;
    }

    static interface Context {
        boolean tryMinus(ContextItem context);

        void recovery(ContextItem context);
    }

    static interface ContextItem {
    }

    static interface ContextLimit {
        boolean tryLimit(ContextItem context);
    }

    public void putLimit(T key, ContextLimit contextLimit) {
        map.put(key, contextLimit);
    }

    public void remove(T key) {
        map.remove(key);
    }

    public void push(T key, ContextItem context, Promise<Void> promise, Future<Void> endFuture) {
        Entry<T> entry = new Entry<>();
        entry.key = key;
        entry.context = context;
        entry.promise = promise;
        entry.endFuture = endFuture;
        queue.add(entry);
    }

    public Future<Void> take(T key, ContextItem context, Future<Void> endFuture) {
        return Future.future(promise -> take(key, context, promise, endFuture));
    }

    public void take(T key, ContextItem context, Promise<Void> promise, Future<Void> endFuture) {
        endFuture.onComplete(voidAsyncResult -> {
            global.recovery(context);
            Entry entry = queue.poll();
            if (entry != null) {
                take((T) entry.key, entry.context, entry.promise, entry.endFuture);
            }
        });
        if (map.isEmpty()) {
            globalLimit(context, key, promise, endFuture);
            return;
        }
        ContextLimit contextLimit = map.get(key);
        if (contextLimit == null) {
            globalLimit(context, key, promise, endFuture);
        } else if (contextLimit.tryLimit(context)) {
            globalLimit(context, key, promise, endFuture);
        } else {
            push(key, context, promise, endFuture);
        }
    }

    private void globalLimit(ContextItem context, T key, Promise<Void> promise, Future<Void> endFuture) {
        if (global.tryMinus(context)) {
            promise.tryComplete();
        } else {
            push(key, context, promise, endFuture);
        }
    }

}
