package io.mycat.sqlhandler.config;

import io.mycat.config.DatasourceConfig;
import io.mycat.config.KVObject;
import io.mycat.util.JsonUtil;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DbKVImpl<T extends KVObject> implements KV<T> {
    final DatasourceConfig datasourceConfig;
    private String path;
    final Class aClass;

    public DbKVImpl(DatasourceConfig datasourceConfig, String path, Class aClass) {
        this.datasourceConfig = datasourceConfig;
        this.path = path;
        this.aClass = aClass;
    }

    @Override
    public Optional<T> get(String key) {
        Config config = DbStorageManagerImpl.readConfig(datasourceConfig);
        Map<String, String> stringStringMap = config.config.get(this.path);
        if (stringStringMap == null) return null;
        Optional<String> sOptional = Optional.ofNullable(stringStringMap.get(key));
        return sOptional.map((Function<String, T>) s -> {
            return ((T)JsonUtil.from(s, aClass));
        });
    }

    @Override
    public void removeKey(String key) {
        Config config = DbStorageManagerImpl.readConfig(datasourceConfig);
        Map<String, String> stringStringMap = new HashMap<>(config.config.getOrDefault(this.path, Collections.emptyMap()));
        stringStringMap.remove(key);
        config.config.put(this.path,stringStringMap);
        DbStorageManagerImpl.removeBy(datasourceConfig,config.version);
        DbStorageManagerImpl.writeString(datasourceConfig, config.config);
    }

    @Override
    public void put(String key, T value) {
        Config config = DbStorageManagerImpl.readConfig(datasourceConfig);
        Map<String, String> stringStringMap = new HashMap<>(config.config.getOrDefault(this.path, Collections.emptyMap()));
        stringStringMap.put(key, JsonUtil.toJson(value));
        config.config.put(this.path,stringStringMap);
        DbStorageManagerImpl.writeString(datasourceConfig, config.config);
    }

    @Override
    public List<T> values() {
        Config config = DbStorageManagerImpl.readConfig(datasourceConfig);
        Map<String, String> stringStringMap = config.config.getOrDefault(this.path, Collections.emptyMap());
        return (List) stringStringMap.values().stream().map(i -> JsonUtil.from(i, aClass)).collect(Collectors.toList());
    }

    public void syncAll(Map<String,Map<String,String>> config){
        DbStorageManagerImpl.writeString(datasourceConfig,config);
    }
}
