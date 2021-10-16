package io.mycat.sqlhandler.config;

import io.mycat.config.KVObject;

import java.util.List;

public interface KV<T extends KVObject> {
    T get(String key);
    void removeKey(String key);

    void put(String key, T value);

    public List<T> values();
}