/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.sqlhandler;

import io.mycat.MetaClusterCurrent;
import io.mycat.config.*;
import io.mycat.sqlhandler.config.FileStorageManagerImpl;
import io.mycat.sqlhandler.config.StdStorageManagerImpl;
import io.mycat.sqlhandler.config.StorageManager;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.util.List;

public class ConfigUpdater {
    public static MycatRouterConfigOps getOps() {
        return getOps(Options.builder().persistence(true).build());
    }

    public static MycatRouterConfigOps getOps(Options options) {
        StorageManager metadataManager = MetaClusterCurrent.wrapper(StorageManager.class);
        MycatRouterConfig routerConfig = getRuntimeConfigSnapshot();
        return new MycatRouterConfigOps(routerConfig, metadataManager, options);
    }

    @SneakyThrows
    public static void loadConfigFromFile() {
        load(getFileConfigSnapshot());
    }

    @SneakyThrows
    public static void writeDefaultConfigToFile() {
        MycatRouterConfigOps mycatRouterConfigOps = getOps();
        mycatRouterConfigOps.reset();
        mycatRouterConfigOps.commit();
    }

    @SneakyThrows
    public static void load(MycatRouterConfig mycatRouterConfig) {
        StorageManager storageManager = MetaClusterCurrent.wrapper(StorageManager.class);
        MycatRouterConfig orginal = MetaClusterCurrent.exist(MycatRouterConfig.class) ?
                MetaClusterCurrent.wrapper(MycatRouterConfig.class) : new MycatRouterConfig();
        MycatRouterConfigOps mycatRouterConfigOps = new MycatRouterConfigOps(orginal,
                storageManager,
                Options.builder()
                        .persistence(false).build(), mycatRouterConfig);
        mycatRouterConfigOps.commit();
    }

    public static StorageManager createStorageManager(Path basePath) {
        FileStorageManagerImpl fileStorageManager = new FileStorageManagerImpl(basePath);
        return new StdStorageManagerImpl(fileStorageManager);
    }


    @NotNull
    public static MycatRouterConfig getRuntimeConfigSnapshot() {
        return MetaClusterCurrent.exist(MycatRouterConfig.class)? MetaClusterCurrent.wrapper(MycatRouterConfig.class):new MycatRouterConfig();
    }

    @NotNull
    public static MycatRouterConfig getFileConfigSnapshot() {
        MycatRouterConfig mycatRouterConfig = new MycatRouterConfig();
        StorageManager metadataManager = MetaClusterCurrent.wrapper(StorageManager.class);
        List<LogicSchemaConfig> logicSchemaConfigs = metadataManager.get(LogicSchemaConfig.class).values();
        List<ClusterConfig> clusterConfigs = metadataManager.get(ClusterConfig.class).values();
        List<DatasourceConfig> datasourceConfigs = metadataManager.get(DatasourceConfig.class).values();
        List<UserConfig> userConfigs = metadataManager.get(UserConfig.class).values();
        List<SequenceConfig> sequenceConfigs = metadataManager.get(SequenceConfig.class).values();
        List<SqlCacheConfig> sqlCacheConfigs = metadataManager.get(SqlCacheConfig.class).values();

        mycatRouterConfig.setSchemas(logicSchemaConfigs);
        mycatRouterConfig.setClusters(clusterConfigs);
        mycatRouterConfig.setDatasources(datasourceConfigs);
        mycatRouterConfig.setUsers(userConfigs);
        mycatRouterConfig.setSequences(sequenceConfigs);
        mycatRouterConfig.setSqlCacheConfigs(sqlCacheConfigs);
        return mycatRouterConfig;
    }
}
