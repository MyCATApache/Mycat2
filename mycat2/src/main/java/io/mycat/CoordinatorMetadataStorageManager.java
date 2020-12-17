package io.mycat;

import io.mycat.config.*;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.util.JsonUtil;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class CoordinatorMetadataStorageManager extends MetadataStorageManager {

    public CoordinatorMetadataStorageManager(MycatServerConfig serverConfig, Store store,
                                             ConfigReaderWriter readerWriter,
                                             String datasourceProvider) {
        this.serverConfig = serverConfig;
        this.store = store;
        this.readerWriter = readerWriter;
        this.datasourceProvider = datasourceProvider;

    }

    private MycatServerConfig serverConfig;
    final Store store;
    final FileMetadataStorageManager.State state = new FileMetadataStorageManager.State();
    final ConfigReaderWriter readerWriter;
    final String datasourceProvider;

    @Override
    void start() throws Exception {
        try (ConfigOps configOps = startOps()) {
            configOps.commit(new MycatRouterConfigOps(loadFromLocalConfigCenter(), configOps));
        }

        store.addChangedCallback(new ChangedValueCallback() {
            @Override
            public String getKey() {
                return "schemas";
            }

            @Override
            public void onRemove(String path) throws Exception {
                String schemaName =path;
                try (MycatRouterConfigOps ops = ConfigUpdater.getOps(CoordinatorMetadataStorageManager.this)) {
                        ops.dropSchema(schemaName);
                    ops.commit();
                }
            }

            @Override
            public void onPut(String path, String text) throws Exception {
                String schemaName =path;
                try (MycatRouterConfigOps ops = ConfigUpdater.getOps(CoordinatorMetadataStorageManager.this)) {
                    ops.putSchema(JsonUtil.from(text, LogicSchemaConfig.class));
                    ops.commit();
                }
            }
        });
        store.addChangedCallback(new ChangedValueCallback() {
            @Override
            public String getKey() {
                return "datasources";
            }

            @Override
            public void onRemove(String path) throws Exception {
                try (MycatRouterConfigOps ops = ConfigUpdater.getOps(CoordinatorMetadataStorageManager.this)) {
                    ops.removeDatasource(path);
                    ops.commit();
                }
            }

            @Override
            public void onPut(String path, String text) throws Exception {
                try (MycatRouterConfigOps ops = ConfigUpdater.getOps(CoordinatorMetadataStorageManager.this)) {
                    ops.putDatasource(JsonUtil.from(text, DatasourceConfig.class));
                    ops.commit();
                }
            }


        });
        store.addChangedCallback(new ChangedValueCallback() {
            @Override
            public String getKey() {
                return "clusters";
            }

            @Override
            public void onRemove(String path) throws Exception {
                try (MycatRouterConfigOps ops = ConfigUpdater.getOps(CoordinatorMetadataStorageManager.this)) {
                    ops.removeReplica(path);
                    ops.commit();
                }
            }

            @Override
            public void onPut(String path, String text) throws Exception {
                try (MycatRouterConfigOps ops = ConfigUpdater.getOps(CoordinatorMetadataStorageManager.this)) {
                    ops.putReplica(JsonUtil.from(text, ClusterConfig.class));
                    ops.commit();
                }
            }

        });
        store.addChangedCallback(new ChangedValueCallback() {
            @Override
            public String getKey() {
                return "sequences";
            }

            @Override
            public void onRemove(String path) throws Exception {
                try (MycatRouterConfigOps ops = ConfigUpdater.getOps(CoordinatorMetadataStorageManager.this)) {
                    ops.removeSequenceByName(path);
                    ops.commit();
                }
            }

            @Override
            public void onPut(String path, String text) throws Exception {
                try (MycatRouterConfigOps ops = ConfigUpdater.getOps(CoordinatorMetadataStorageManager.this)) {
                    ops.putSequence(JsonUtil.from(text, SequenceConfig.class));
                    ops.commit();
                }
            }
        });
        store.addChangedCallback(new ChangedValueCallback() {
            @Override
            public String getKey() {
                return "users";
            }

            @Override
            public void onRemove(String path) throws Exception {
                try (MycatRouterConfigOps ops = ConfigUpdater.getOps(CoordinatorMetadataStorageManager.this)) {
                        ops.deleteUser(path);
                    ops.commit();
                }
            }

            @Override
            public void onPut(String path, String text) throws Exception {
                try (MycatRouterConfigOps ops = ConfigUpdater.getOps(CoordinatorMetadataStorageManager.this)) {
                        ops.putUser(JsonUtil.from(text, UserConfig.class));
                    ops.commit();
                }
            }

        });
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
        store.commit();
        return routerConfig;
    }


    @Override
    public void reportReplica(String name, Set<String> dsNames) {
        state.replica.put(name, dsNames);
        store.set("state", ConfigReaderWriter.getReaderWriterBySuffix("json")
                .transformation(state));
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
            public void commit(Object ops) {
                MycatRouterConfigOps routerConfig = (MycatRouterConfigOps) ops;
                MycatRouterConfig mycatRouterConfig = routerConfig.getMycatRouterConfig();
                ConfigPrepareExecuter prepare = new ConfigPrepareExecuter(routerConfig, CoordinatorMetadataStorageManager.this, datasourceProvider);
                prepare.prepareRuntimeObject();
                prepare.prepareStoreDDL();

//                if (routerConfig.isUpdateSchemas()) {
                store.set("schemas",
                        mycatRouterConfig
                                .getSchemas()
                                .stream()
                                .collect(Collectors
                                        .toMap(k -> k.getSchemaName(), v -> readerWriter.transformation(v))));
//                }
//                if (routerConfig.isUpdateClusters()) {
                store.set("clusters",
                        mycatRouterConfig
                                .getClusters()
                                .stream()
                                .collect(Collectors
                                        .toMap(k -> k.getName(), v -> readerWriter.transformation(v))));
//                }
//                if (routerConfig.isUpdateDatasources()) {
                store.set("datasources",
                        mycatRouterConfig
                                .getDatasources()
                                .stream()
                                .collect(Collectors
                                        .toMap(k -> k.getName(), v -> readerWriter.transformation(v))));
//                }
//                if (routerConfig.isUpdateUsers()) {
                store.set("users",
                        mycatRouterConfig
                                .getUsers()
                                .stream()
                                .collect(Collectors
                                        .toMap(k -> k.getUsername(), v -> readerWriter.transformation(v))));

//                }
//                if (routerConfig.isUpdateSequences()) {
                store.set("sequences",
                        mycatRouterConfig
                                .getSequences()
                                .stream()
                                .collect(Collectors
                                        .toMap(k -> k.getName(), v -> readerWriter.transformation(v))));
//                }
//                if (routerConfig.isUpdatePrototype()) {
//                    store.set("prototype",
//                            mycatRouterConfig.getPrototype()
//                    );
//                }


                store.set("state", readerWriter.transformation(state));
                store.commit();
                prepare.commit();
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
