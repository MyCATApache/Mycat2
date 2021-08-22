package io.mycat.config;

import io.mycat.ConfigOps;
import io.mycat.MetadataStorageManager;

import java.util.List;
import java.util.Map;

public class AssembleMetadataStorageManager extends MetadataStorageManager {
    MetadataStorageManager dbManager;
    MetadataStorageManager fileManager;

    public AssembleMetadataStorageManager(MetadataStorageManager fileManager, MetadataStorageManager dbManager) {

        this.fileManager = fileManager;
        this.dbManager = dbManager;
    }

    @Override
    public void start() throws Exception {
        fileManager.start();
    }

    @Override
    public void start(MycatRouterConfig mycatRouterConfig) {
        fileManager.start(mycatRouterConfig);
    }

    @Override
    public void reportReplica(Map<String, List<String>> dsNames) {
        fileManager.reportReplica(dsNames);
        if (dbManager != null) {
            dbManager.reportReplica(dsNames);
        }
    }

    @Override
    public ConfigOps startOps() {
        return fileManager.startOps();
    }

    @Override
    public MycatRouterConfig fetchFromStore() {
        return fileManager.fetchFromStore();
    }

    @Override
    public void sync(MycatRouterConfig mycatRouterConfig, State state) {
        fileManager.sync(mycatRouterConfig, state);
        if (dbManager != null) {
            dbManager.sync(mycatRouterConfig, state);
        }
    }

    public MycatRouterConfig fetchConfigFromDb() {
        return dbManager.fetchFromStore();
    }

    public MycatRouterConfig fetchFileFromFile() {
        return fileManager.fetchFromStore();
    }

    public void syncConfigFromFileToDb() {
        MycatRouterConfig mycatRouterConfig = fileManager.fetchFromStore();
        if (dbManager != null) {
            dbManager.sync(mycatRouterConfig, new State());
        }
    }

    public void syncConfigFromDbToFile() {
        if (dbManager != null) {
            MycatRouterConfig mycatRouterConfig = dbManager.fetchFromStore();
            fileManager.sync(mycatRouterConfig, new State());
        }

    }

    public boolean check() {
        if (dbManager != null) {
            MycatRouterConfig one = fileManager.fetchFromStore();
            MycatRouterConfig two = dbManager.fetchFromStore();
            return one.equals(two);
        }
        return true;
    }
}
