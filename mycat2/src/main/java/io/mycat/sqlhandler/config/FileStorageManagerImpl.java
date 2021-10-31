package io.mycat.sqlhandler.config;

import io.mycat.config.DatasourceConfig;
import io.mycat.config.KVObject;
import io.mycat.util.JsonUtil;
import lombok.SneakyThrows;
import org.jetbrains.annotations.Nullable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

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
        return  new HashSet<>(registerClassSet);
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
        FileKV.writeFile(JsonUtil.toJson(state),statePath);
    }

    @Nullable
    @Override
    public Optional<DatasourceConfig> getPrototypeDatasourceConfig() {
        return Optional.empty();
    }
}
