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
package io.mycat.config;

import io.mycat.*;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import org.apache.curator.framework.CuratorFramework;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CoordinatorMetadataStorageManager extends MetadataStorageManager {


    private FileMetadataStorageManager storageManager;
    private final ZKStore store;
    final ConfigReaderWriter readerWriter = ConfigReaderWriter.getReaderWriterBySuffix("json");

    public CoordinatorMetadataStorageManager(FileMetadataStorageManager storageManager,
                                             CuratorFramework curatorFramework) throws Exception {
        this.storageManager = storageManager;
        this.store = new ZKStore(curatorFramework, this);

    }

    @Override
    public void start() throws Exception {
        this.store.init();
        try (ConfigOps configOps = startOps()) {
            configOps.commit(new MycatRouterConfigOps(loadFromLocalConfigCenter(), configOps));
        }
        this.store.listen();

    }

    @Override
    public void reportReplica(Map<String, List<String>> dsNames) {
        storageManager.reportReplica(dsNames);
        FileMetadataStorageManager.State state = new FileMetadataStorageManager.State();
        store.set("state", readerWriter.transformation(state));
    }

    private MycatRouterConfig loadFromLocalConfigCenter() {
        Map<String, String> schemas = store.getMap("schemas");
        Map<String, String> clusters = store.getMap("clusters");
        Map<String, String> datasources = store.getMap("datasources");
        Map<String, String> users = store.getMap("users");
        Map<String, String> sequences = store.getMap("sequences");
        String prototype = store.get("prototype");

        if (schemas == null
                && clusters == null
                && datasources == null
                && users == null
                && sequences == null
                && prototype == null) {
            store.begin();
            MycatRouterConfig defaultRouterConfig = new MycatRouterConfig();

            store.set("schemas", schemas =
                    defaultRouterConfig
                            .getSchemas()
                            .stream()
                            .collect(Collectors
                                    .toMap(k -> k.getSchemaName(), v -> readerWriter.transformation(v))));

            store.set("datasources", sequences = defaultRouterConfig.getDatasources()
                    .stream()
                    .collect(Collectors
                            .toMap(k -> k.getName(), v -> readerWriter.transformation(v))));

            store.set("clusters", clusters =
                    defaultRouterConfig
                            .getClusters()
                            .stream()
                            .collect(Collectors
                                    .toMap(k -> k.getName(), v -> readerWriter.transformation(v))));

            store.set("users", users =
                    defaultRouterConfig.getUsers()
                            .stream()
                            .collect(Collectors
                                    .toMap(k -> k.getUsername(), v -> readerWriter.transformation(v))));

            store.set("sequences", sequences = defaultRouterConfig.getSequences()
                    .stream()
                    .collect(Collectors
                            .toMap(k -> k.getName(), v -> readerWriter.transformation(v))));
//
//            store.set("prototype", prototype = defaultRouterConfig.getPrototype());

        }

        List<LogicSchemaConfig> logicSchemaConfigs = Optional
                .ofNullable(schemas)
                .orElse(Collections.emptyMap())
                .values()
                .stream()
                .map(i -> readerWriter.transformation(i, LogicSchemaConfig.class))
                .collect(Collectors.toList());

        List<ClusterConfig> clusterConfigs = Optional.ofNullable(clusters)
                .orElse(Collections.emptyMap())
                .values()
                .stream()
                .map(i -> readerWriter.transformation(i, ClusterConfig.class))
                .collect(Collectors.toList());

        List<DatasourceConfig> datasourceConfigs = Optional.ofNullable(datasources)
                .orElse(Collections.emptyMap())
                .values()
                .stream()
                .map(i -> readerWriter.transformation(i, DatasourceConfig.class))
                .collect(Collectors.toList());

        List<UserConfig> userConfigs = Optional.ofNullable(users)
                .orElse(Collections.emptyMap())
                .values()
                .stream()
                .map(i -> readerWriter.transformation(i, UserConfig.class))
                .collect(Collectors.toList());

        List<SequenceConfig> sequenceList = Optional.ofNullable(sequences)
                .orElse(Collections.emptyMap())
                .values()
                .stream()
                .map(i -> readerWriter.transformation(i, SequenceConfig.class))
                .collect(Collectors.toList());

        prototype = "prototype";
        MycatRouterConfig routerConfig = new MycatRouterConfig();
        routerConfig.setSchemas(logicSchemaConfigs);
        routerConfig.setClusters(clusterConfigs);
        routerConfig.setDatasources(datasourceConfigs);
        routerConfig.setPrototype(Objects.requireNonNull(prototype));
        routerConfig.setUsers(userConfigs);
        routerConfig.setSequences(sequenceList);
        FileMetadataStorageManager.defaultConfig(routerConfig);
        storageManager.start();
        store.commit();
        return routerConfig;
    }


    @Override
    public ConfigOps startOps() {
        store.begin();
        return new ConfigOps() {
            @Override
            public Object currentConfig() {
                return MetaClusterCurrent.wrapper(MycatRouterConfig.class);
            }

            @Override
            @SneakyThrows
            public void commit(Object ops) {
                MycatRouterConfigOps routerConfig = (MycatRouterConfigOps) ops;
                MycatRouterConfig mycatRouterConfig = routerConfig.getMycatRouterConfig();
                FileMetadataStorageManager.State state = storageManager.commitAndSyncDisk(routerConfig);
                {
                    store.set("schemas",
                            mycatRouterConfig
                                    .getSchemas()
                                    .stream()
                                    .collect(Collectors
                                            .toMap(k -> k.getSchemaName(), v -> readerWriter.transformation(v))));
//                }
                    store.set("clusters",
                            mycatRouterConfig
                                    .getClusters()
                                    .stream()
                                    .collect(Collectors
                                            .toMap(k -> k.getName(), v -> readerWriter.transformation(v))));
//                }
                    store.set("datasources",
                            mycatRouterConfig
                                    .getDatasources()
                                    .stream()
                                    .collect(Collectors
                                            .toMap(k -> k.getName(), v -> readerWriter.transformation(v))));
//                }
                    store.set("users",
                            mycatRouterConfig
                                    .getUsers()
                                    .stream()
                                    .collect(Collectors
                                            .toMap(k -> k.getUsername(), v -> readerWriter.transformation(v))));

//                }
                    store.set("sequences",
                            mycatRouterConfig
                                    .getSequences()
                                    .stream()
                                    .collect(Collectors
                                            .toMap(k -> k.getName(), v -> readerWriter.transformation(v))));

                    store.set("sqlcaches",
                            mycatRouterConfig
                                    .getSqlCacheConfigs()
                                    .stream()
                                    .collect(Collectors
                                            .toMap(k -> k.getName(), v -> readerWriter.transformation(v))));


                    store.set("state", readerWriter.transformation(state));
                    store.commit();
                }
            }

            @Override
            public void close() {
                store.close();
            }
        };
    }

    interface Store {

        void addChangedCallback(ChangedValueCallback changedCallback);

        void begin();

        String get(String schema);

        void set(String schemas, String transformation);

        void set(String schemas, Map<String, String> transformation);

        Map<String, String> getMap(String schemas);

        void commit();

        void close();
    }

    interface ChangedValueCallback {
        String getKey();

        void onRemove(String path) throws Exception;

        void onPut(String path, String text) throws Exception;
    }
}
