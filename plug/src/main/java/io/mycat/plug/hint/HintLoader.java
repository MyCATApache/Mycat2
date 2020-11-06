package io.mycat.plug.hint;

import io.mycat.Hint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public enum HintLoader {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(HintLoader.class);
    final ConcurrentMap<String, Hint> map = new ConcurrentHashMap<>();
    public Hint get(String name) {
        return map.get(name);
    }

    public Hint getOrDefault(String name, Hint defaultHint) {
        return map.getOrDefault(name, defaultHint);
    }
    public void register(String name, Hint hint) {
        map.put(name, hint);
    }

}