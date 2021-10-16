package io.mycat.sqlhandler.config;

import io.mycat.config.ClusterConfig;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.KVObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class StdStorageManagerImpl implements StorageManager {
    final StorageManager fileStorageManager;

    public StdStorageManagerImpl(StorageManager fileStorageManager) {
        this.fileStorageManager = fileStorageManager;
    }
    private  Optional<DatasourceConfig> getPrototypeDatasourceConfig(){
        return getPrototypeDatasourceConfig(fileStorageManager);
    }
    @Nullable
    public static Optional<DatasourceConfig> getPrototypeDatasourceConfig(StorageManager fileStorageManager) {
        KV<ClusterConfig> clusterConfigKV = fileStorageManager.get(ClusterConfig.class);
        KV<DatasourceConfig> datasourceConfigKV = fileStorageManager.get(DatasourceConfig.class);
        Optional<ClusterConfig> prototypeOptional = Optional.ofNullable(clusterConfigKV.get("prototype"));
        DatasourceConfig configPrototypeDs = prototypeOptional.flatMap(clusterConfig -> {

            List<String> masters = Optional.ofNullable(clusterConfig.getMasters()).orElse(Collections.emptyList());
            List<String> replicas = Optional.ofNullable(clusterConfig.getReplicas()).orElse(Collections.emptyList());

            List<String> strings = new ArrayList<>();
            strings.addAll(masters);
            strings.addAll(replicas);

            return strings.stream().map(i -> datasourceConfigKV.get(i)).filter(i -> i != null).findFirst();
        }).orElse(Optional.ofNullable(datasourceConfigKV.get("prototype")).orElse(datasourceConfigKV.get("prototypeDs")));
        if (configPrototypeDs == null) {
            List<DatasourceConfig> values = datasourceConfigKV.values();
            if (values.isEmpty()) {
                //不开启db
            } else {
                configPrototypeDs = values.get(0);
            }
        }
        return Optional.ofNullable(configPrototypeDs);
    }


    @Override
    public void register(Class aClass) {
        fileStorageManager.register(aClass);
    }

    @Override
    public <T extends KVObject> KV<T> get(String path, String fileNameTemplate, Class<T> aClass) {
        return fileStorageManager.get(path, fileNameTemplate, aClass);
    }


    @Override
    public Collection<Class> registerClasses() {
        return fileStorageManager.registerClasses();
    }

    @Override
    public void syncFromNet() {
        Optional<DatasourceConfig> prototypeDatasourceConfig = getPrototypeDatasourceConfig(fileStorageManager);
        Objects.requireNonNull(prototypeDatasourceConfig)
                .ifPresent(datasourceConfig -> {
                    DbStorageManagerImpl dbStorageManager = getDbStorageManager(datasourceConfig);

                    sync(dbStorageManager, fileStorageManager);
                });
    }

    @NotNull
    private DbStorageManagerImpl getDbStorageManager(DatasourceConfig datasourceConfig) {
        DbStorageManagerImpl dbStorageManager = new DbStorageManagerImpl(datasourceConfig);
        for (Class registerClass : registerClasses()) {
            dbStorageManager.register(registerClass);
        }
        return dbStorageManager;
    }

    private static void sync(StorageManager from, StorageManager to) {
        for (Class registerClass : from.registerClasses()) {
            KV netKv = from.get(registerClass);
            KV localKv = to.get(registerClass);
            List<KVObject> values = netKv.values();
            for (KVObject value : values) {
                localKv.put(value.keyName(),value);
            }
        }
    }

    @Override
    public void syncToNet() {
        Optional<DatasourceConfig> prototypeDatasourceConfig = getPrototypeDatasourceConfig(fileStorageManager);
        Objects.requireNonNull(prototypeDatasourceConfig).ifPresent(datasourceConfig -> {
            sync(fileStorageManager,   getDbStorageManager(datasourceConfig));
        });
    }

    @Override
    public void reportReplica(Map<String, List<String>> state) {
        fileStorageManager.reportReplica(state);
        Optional<DatasourceConfig> prototypeDatasourceConfig = getPrototypeDatasourceConfig(fileStorageManager);
        prototypeDatasourceConfig.ifPresent(datasourceConfig -> new DbStorageManagerImpl(datasourceConfig).reportReplica(state));
    }
}
