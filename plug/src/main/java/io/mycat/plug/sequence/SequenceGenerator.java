package io.mycat.plug.sequence;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public enum SequenceGenerator {
    INSTANCE;
    final ConcurrentMap<String, LongSupplier> map = new ConcurrentHashMap<>();
    final LongSupplier defaultSequenceGenerator = new LongSupplier() {
        final AtomicLong counter = new AtomicLong();

        @Override
        public long getAsLong() {
            return counter.getAndIncrement();
        }
    };

    public void register(String key, LongSupplier supplier) {
        map.put(key, supplier);
    }

    public long next(String key) {
        if (key == null) {
            return defaultSequenceGenerator.getAsLong();
        } else {
            return map.getOrDefault(key, defaultSequenceGenerator).getAsLong();
        }
    }
}