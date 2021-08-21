package io.mycat.config;

import io.mycat.ConfigOps;
import io.mycat.MetadataStorageManager;

import java.util.List;
import java.util.Map;

public class AssembleMetadataStorageManager extends MetadataStorageManager {
    MetadataStorageManager dbManager;
    MetadataStorageManager fileManager;

    public AssembleMetadataStorageManager(MetadataStorageManager dbManager, MetadataStorageManager fileManager) {
        this.dbManager = dbManager;
        this.fileManager = fileManager;
    }

    @Override
    public void start() throws Exception {
        fileManager.start();
    }

    @Override
    public void reportReplica(Map<String, List<String>> dsNames) {
        dbManager.reportReplica(dsNames);
        fileManager.reportReplica(dsNames);
    }

    @Override
    public ConfigOps startOps() {
        return dbManager.startOps();
    }

    @Override
    public MycatRouterConfig fetchFromStore() {
        return dbManager.fetchFromStore();
    }

    @Override
    public void sync(MycatRouterConfig mycatRouterConfig, State state) {
        dbManager.sync(mycatRouterConfig, state);
        fileManager.sync(mycatRouterConfig, state);
    }

    public MycatRouterConfig fetchConfigFromDb() {
        return dbManager.fetchFromStore();
    }

    public MycatRouterConfig fetchFileFromFile() {
        return fileManager.fetchFromStore();
    }

    public void syncConfigFromFileToDb() {
        MycatRouterConfig mycatRouterConfig = fileManager.fetchFromStore();
        dbManager.sync(mycatRouterConfig, new State());
    }

    public void syncConfigFromDbToFile() {
        MycatRouterConfig mycatRouterConfig = dbManager.fetchFromStore();
        fileManager.sync(mycatRouterConfig, new State());
    }

    public boolean check() {
        MycatRouterConfig one = fileManager.fetchFromStore();
        MycatRouterConfig two = dbManager.fetchFromStore();
        return one.equals(two);
    }
}
