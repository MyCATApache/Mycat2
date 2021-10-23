package io.mycat.sqlhandler.config;

import io.mycat.config.KVObject;

import java.util.List;
import java.util.Optional;

public interface KV<T extends KVObject> {
    Optional<T> get(String key);
    void removeKey(String key);

    void put(String key, T value);

    public List<T> values();

    default void clear(){
        List<T> values = values();
        for (T value : values) {
            removeKey(value.keyName());
        }

    }
}