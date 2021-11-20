package io.mycat.sqlhandler.config;

import io.mycat.config.DatasourceConfig;
import io.mycat.config.KVObject;
import io.mycat.util.JsonUtil;
import lombok.SneakyThrows;
import org.jetbrains.annotations.Nullable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FileStorageManagerImpl implements StorageManager {
    private static String suffix = "json";
    private final Path baseDirectory;
    private final Set<Class> registerClassSet = Collections.newSetFromMap(new IdentityHashMap<>());

    public FileStorageManagerImpl(Path baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    @Override
    public void register(Class aClass) {
        registerClassSet.add(aClass);
    }

    @Override
    @SneakyThrows
    public <T extends KVObject> KV<T> get(String path, String fileNameTemplate, Class<T> aClass) {
        Path dir = baseDirectory.resolve(path);
        if (Files.notExists(dir)) Files.createDirectory(dir);
        KV<T> kv = getFilekv(fileNameTemplate, aClass, dir);
        return kv;
    }


    @Override
    public Collection<Class> registerClasses() {
        return new HashSet<>(registerClassSet);
    }

    @Override
    public void syncFromNet() {

    }

    @Override
    public void syncToNet() {

    }

    @Override
    public boolean checkConfigConsistency() {
        return true;
    }

    private <T extends KVObject> KV<T> getFilekv(String fileNameTemplate, Class<T> aClass, Path dir) {
        return new FileKV(fileNameTemplate, aClass, dir, suffix);
    }

    @Override
    public void reportReplica(Map<String, List<String>> state) {
        Path statePath = baseDirectory.resolve("state.json");
        FileKV.writeFile(JsonUtil.toJson(state), statePath);
    }

    @Nullable
    @Override
    public Optional<DatasourceConfig> getPrototypeDatasourceConfig() {
        return Optional.empty();
    }

    public Map<String, Map<String, String>> toMap() {
        Map<String, Map<String, String>> result = this.registerClasses().stream().collect(Collectors.toMap(k -> {
            try {
                return ((KVObject) k.newInstance()).path();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            return null;
        }, new Function<Class, Map<String, String>>() {
            @Override
            public Map<String, String> apply(Class aClass) {
                List<KVObject> values = get(aClass).values();
                Map<String, String> collect1 = values.stream().collect(Collectors.toMap(k -> k.keyName(), v2 -> JsonUtil.toJson(v2)));
                return collect1;
            }
        }));
        return result;
    }

    public void write(Map<String, Map<String, String>> map) {
        Map<String, Class> pathToMap = registerClassSet.stream().collect(Collectors.toMap(k -> {
            try {
                return ((KVObject) k.newInstance()).path();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            return null;
        }, v -> v));
        Set<Map.Entry<String, Map<String, String>>> entries = map.entrySet();
        for (Map.Entry<String, Map<String, String>> entry : entries) {
            Class aClass = pathToMap.get(entry.getKey());
            KV kv = get(aClass);
            for (Map.Entry<String, String> stringStringEntry : entry.getValue().entrySet()) {
                kv.put(stringStringEntry.getKey(), (KVObject) JsonUtil.from(stringStringEntry.getValue(), aClass));
            }
        }

    }
}
