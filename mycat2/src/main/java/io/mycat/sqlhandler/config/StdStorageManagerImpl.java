package io.mycat.sqlhandler.config;

import io.mycat.MycatException;
import io.mycat.config.ClusterConfig;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.KVObject;
import io.mycat.sqlhandler.ddl.SQLCallStatementHandler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class StdStorageManagerImpl implements StorageManager {
    final StorageManager fileStorageManager;
    private static final Logger LOGGER = LoggerFactory.getLogger(StdStorageManagerImpl.class);

    public StdStorageManagerImpl(StorageManager fileStorageManager) {
        this.fileStorageManager = fileStorageManager;
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
        Optional<DbStorageManagerImpl> dbStorageManagerOptional = getDbStorageManager();
        dbStorageManagerOptional.ifPresent(dbStorageManager -> sync(dbStorageManager, fileStorageManager));
    }

    @NotNull
    public DbStorageManagerImpl getDbStorageManager(DatasourceConfig datasourceConfig) {
        DbStorageManagerImpl dbStorageManager = new DbStorageManagerImpl(datasourceConfig);
        for (Class registerClass : registerClasses()) {
            dbStorageManager.register(registerClass);
        }
        return dbStorageManager;
    }

    public Optional<DbStorageManagerImpl> getDbStorageManager() {
        Optional<DatasourceConfig> prototypeDatasourceConfig = getPrototypeDatasourceConfig(fileStorageManager);
        return Objects.requireNonNull(prototypeDatasourceConfig)
                .map(datasourceConfig -> {
                    return getDbStorageManager(datasourceConfig);
                });
    }

    public static void sync(StorageManager from, StorageManager to) {
        to.write(from.toMap());
        boolean b = checkConfigConsistency(from, to);
        if (!b) {
            throw new MycatException("sync fail");
        }
    }

    private static boolean checkConfigConsistency(StorageManager from, StorageManager to) {
        //去掉大小为0的value
        Map<String, Map<String, String>> fromMap = from.toMap()
                .entrySet().stream().filter(i -> !i.getValue().isEmpty())
                .collect(Collectors.toMap(k->k.getKey(),v->v.getValue()));
        Map<String, Map<String, String>> toMap = to.toMap()
                .entrySet().stream().filter(i -> !i.getValue().isEmpty())
                .collect(Collectors.toMap(k->k.getKey(),v->v.getValue()));

        return fromMap.equals(toMap)&& fromMap.entrySet().stream().allMatch(new Predicate<Map.Entry<String, Map<String, String>>>() {
            @Override
            public boolean test(Map.Entry<String, Map<String, String>> e) {
                boolean equals = toMap.get(e.getKey()).equals(e.getValue());
                if (equals) {
                    return true;
                } else {
                    LOGGER.error("from {} \n to {}", fromMap, toMap);
                    return false;
                }
            }
        });
    }

    @Override
    public void syncToNet() {
        Optional<DbStorageManagerImpl> dbStorageManagerOptional = getDbStorageManager();
        dbStorageManagerOptional.ifPresent(dbStorageManager -> {
            sync(fileStorageManager, dbStorageManager);
        });
    }

    @Override
    public boolean checkConfigConsistency() {
        Collection<Class> fileClasses = fileStorageManager.registerClasses();
        Optional<DbStorageManagerImpl> dbStorageManagerOptional = getDbStorageManager();
        if (!dbStorageManagerOptional.isPresent()) {
            return false;
        }
        DbStorageManagerImpl dbStorageManager = dbStorageManagerOptional.get();
        Collection<Class> dbClasses = dbStorageManager.registerClasses();
        if (!Objects.equals(fileClasses, dbClasses)) {
            return false;
        }
        return checkConfigConsistency(fileStorageManager, dbStorageManager);
    }

    @Override
    public Map<String, Map<String, String>> toMap() {
        return fileStorageManager.toMap();
    }

    @Override
    public void write(Map<String, Map<String, String>> map) {
        fileStorageManager.write(map);
    }

    @Override
    public void reportReplica(Map<String, List<String>> state) {
        fileStorageManager.reportReplica(state);
        Optional<DbStorageManagerImpl> dbStorageManagerOptional = getDbStorageManager();
        dbStorageManagerOptional.ifPresent(dbStorageManager -> dbStorageManager.reportReplica(state));
    }

    @Nullable
    @Override
    public Optional<DatasourceConfig> getPrototypeDatasourceConfig() {
        return Optional.empty();
    }

    @Nullable
    public static Optional<DatasourceConfig> getPrototypeDatasourceConfig(StorageManager fileStorageManager) {
        KV<ClusterConfig> clusterConfigKV = fileStorageManager.get(ClusterConfig.class);
        KV<DatasourceConfig> datasourceConfigKV = fileStorageManager.get(DatasourceConfig.class);
        Optional<ClusterConfig> prototypeOptional = clusterConfigKV.get("prototype");
        Optional<DatasourceConfig> datasourceConfigOptional = prototypeOptional.flatMap(clusterConfig -> {

            List<String> masters = Optional.ofNullable(clusterConfig.getMasters()).orElse(Collections.emptyList());
            List<String> replicas = Optional.ofNullable(clusterConfig.getReplicas()).orElse(Collections.emptyList());

            List<String> strings = new ArrayList<>();
            strings.addAll(masters);
            strings.addAll(replicas);

            return strings.stream().map(i -> datasourceConfigKV.get(i)).filter(i -> i != null).findFirst();
        }).orElse(Optional.ofNullable(datasourceConfigKV.get("prototype")).orElse(datasourceConfigKV.get("prototypeDs")));
        DatasourceConfig configPrototypeDs = datasourceConfigOptional.orElse(null);
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

}
