package io.mycat.sqlhandler.config;

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
        for (Class registerClass : from.registerClasses()) {
            KV fromKv = from.get(registerClass);
            KV toKv = to.get(registerClass);
            toKv.clear();
            List<KVObject> values = fromKv.values();
            for (KVObject value : values) {
                toKv.put(value.keyName(), value);
            }
        }
        boolean b = checkConfigConsistency(from, to);
        if (!b) {
            throw new UnsupportedOperationException();
        }
    }

    private static boolean checkConfigConsistency(StorageManager from, StorageManager to) {
//        boolean b = true;
//        for (Class aClass : from.registerClasses()) {
//            List left =(List) to.get(aClass).values().stream().sorted().collect(Collectors.toList());
//            List right =(List) from.get(aClass).values().stream().sorted().collect(Collectors.toList());
//
//            if (left.size() != right.size()) {
//                b = false;
//            }
//
//            for (int i = 0;b&& i < left.size(); i++) {
//                Object l = left.get(i);
//                Object r = right.get(i);
//                if (!l.equals(r)) {
//                    b= false;
//                }
//            }
//        }
        return from.registerClasses().stream().parallel().allMatch(aClass -> {
            Object toCollect = to.get(aClass).values().stream().sorted().collect(Collectors.toList());
            Object fromCollect =  from.get(aClass).values().stream().sorted().collect(Collectors.toList());
            if(toCollect .equals(fromCollect)){
                return true;
            }
            LOGGER.error("from {} \n to {}",fromCollect,toCollect);
            return false;
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
    public void reportReplica(Map<String, List<String>> state) {
        fileStorageManager.reportReplica(state);
        Optional<DbStorageManagerImpl> dbStorageManagerOptional = getDbStorageManager();
        dbStorageManagerOptional.ifPresent(dbStorageManager -> dbStorageManager.reportReplica(state));
    }
}
