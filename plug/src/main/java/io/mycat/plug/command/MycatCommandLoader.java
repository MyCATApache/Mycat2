package io.mycat.plug.command;

import io.mycat.Hint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public enum MycatCommandLoader {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatCommandLoader.class);
    final ConcurrentMap<String, Object> map = new ConcurrentHashMap<>();
    public <T> T get(String name) {
        return (T)map.get(name);
    }

    public <T> T  getOrDefault(String name, Object defaultHint) {
        return (T)map.getOrDefault(name, defaultHint);
    }
    public void register(String name, Object hint) {
        map.put(name, hint);
    }
    public void registerIfAbsent(String name, Object hint) {
        map.computeIfAbsent(name, s -> hint);
    }

}