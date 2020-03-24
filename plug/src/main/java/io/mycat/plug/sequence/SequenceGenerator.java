package io.mycat.plug.sequence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

public enum SequenceGenerator {
    INSTANCE;
    final ConcurrentMap<String, Supplier<String>> map = new ConcurrentHashMap<>();
    private final Logger LOGGER = LoggerFactory.getLogger(SequenceGenerator.class);
    final Supplier<String> defaultSequenceGenerator = new Supplier<String>() {
        @Override
        public String get() {
            LOGGER.warn("use default defaultSequenceGenerator may be a error");
            return "9999";
        }
    };

    public void register(String key, Supplier supplier) {
        map.put(key, supplier);
    }

   public Supplier<String> getSequence(String name){
        return map.get(name);
    }
}