package io.mycat.plug.sequence;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public enum SequenceGenerator {
    INSTANCE;
    final ConcurrentMap<String, Supplier<String>> map = new ConcurrentHashMap<>();
    final Supplier<String> defaultSequenceGenerator = new Supplier<String>() {
        @Override
        public String get() {
            return Long.toString(counter.getAndIncrement());
        }

        final AtomicLong counter = new AtomicLong();
    };

    public void register(String key, Supplier supplier) {
        map.put(key, supplier);
    }

    public String next(String key) {
        if (key == null) {
            return defaultSequenceGenerator.get();
        } else {
            return map.getOrDefault(key, defaultSequenceGenerator).get();
        }
    }
}