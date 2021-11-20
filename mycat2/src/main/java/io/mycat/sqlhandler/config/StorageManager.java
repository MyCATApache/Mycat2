package io.mycat.sqlhandler.config;

import io.mycat.ConfigReporter;
import io.mycat.config.KVObject;
import lombok.SneakyThrows;

import java.util.Collection;
import java.util.Map;

public interface StorageManager extends ConfigReporter {
    void register(Class aClass);

    @SneakyThrows
    default public <T extends KVObject> KV<T> get(Class<T> aClass) {
        T t = aClass.newInstance();
        String path = t.path();
        String fileNameTemplate = t.fileName();
        return get(path, fileNameTemplate, aClass);
    }

    public <T extends KVObject> KV<T> get(String path, String fileNameTemplate, Class<T> aClass);

    Collection<Class> registerClasses();

    void syncFromNet();

    void syncToNet();

    boolean checkConfigConsistency();

    public  Map<String, Map<String,String>> toMap();

    public void write(Map<String, Map<String, String>> map);
}
