package io.mycat.sqlhandler.config;

import io.mycat.config.DatasourceConfig;
import io.mycat.config.KVObject;
import io.mycat.util.JsonUtil;
import io.vertx.core.json.Json;

import java.util.*;
import java.util.stream.Collectors;

public class DbKVImpl<T extends KVObject> implements KV<T> {
    final DatasourceConfig datasourceConfig;
    private String path;
    final Class aClass;

    public DbKVImpl(DatasourceConfig datasourceConfig, String key, Class aClass) {
        this.datasourceConfig = datasourceConfig;
        this.path = key;
        this.aClass = aClass;
    }

    @Override
    public T get(String key) {
        Config config = DbStorageManagerImpl.readConfig(datasourceConfig);
        String s = config.config.get(this.path).get(key);
        return (T) JsonUtil.from(s, aClass);
    }

    @Override
    public void removeKey(String key) {
        Config config = DbStorageManagerImpl.readConfig(datasourceConfig);
        Map<String, String> stringStringMap = config.config.getOrDefault(this.path, Collections.emptyMap());
        stringStringMap.remove(key);
        DbStorageManagerImpl.writeString(datasourceConfig, config.config);
    }

    @Override
    public void put(String key, T value) {
        Config config = DbStorageManagerImpl.readConfig(datasourceConfig);
        Map<String, String> stringStringMap = config.config.getOrDefault(this.path, Collections.emptyMap());
        stringStringMap.put(key, Json.encode(value));
        DbStorageManagerImpl.writeString(datasourceConfig, config.config);
    }

    @Override
    public List<T> values() {
        Config config = DbStorageManagerImpl.readConfig(datasourceConfig);
        Map<String, String> stringStringMap = config.config.getOrDefault(this.path, Collections.emptyMap());
        return (List) stringStringMap.values().stream().map(i -> JsonUtil.from(i, aClass)).collect(Collectors.toList());
    }
}
