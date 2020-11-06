package io.mycat;

import io.mycat.config.*;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.util.JsonUtil;
import org.checkerframework.checker.units.qual.C;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class CoordinatorMetadataStorageManager extends MetadataStorageManager {

    public CoordinatorMetadataStorageManager(Store store,
                                             ConfigReaderWriter readerWriter,
                                             String datasourceProvider) {
        this.store = store;
        this.readerWriter = readerWriter;
        this.datasourceProvider = datasourceProvider;

        store.addChangedCallback(new ChangedCallback() {
            @Override
            public String getInterestedPath() {
                return "schemas";
            }

            @Override
            public void onChanged(String path, String text, boolean delete) throws Exception  {
                String schemaName = endPath(path);
                try (MycatRouterConfigOps ops = ConfigUpdater.getOps(CoordinatorMetadataStorageManager.this)) {
                    if (delete) {
                        ops.dropSchema(schemaName);
                    } else {
                        ops.putSchema(JsonUtil.from(text, LogicSchemaConfig.class));
                    }
                    ops.commit();
                }
            }
        });
        store.addChangedCallback(new ChangedCallback() {
            @Override
            public String getInterestedPath() {
                return "datasources";
            }

            @Override
            public void onChanged(String path, String text, boolean delete) throws Exception  {
                String datasourceName = endPath(path);
                try (MycatRouterConfigOps ops = ConfigUpdater.getOps(CoordinatorMetadataStorageManager.this)) {
                    if (delete) {
                        ops.removeDatasource(datasourceName);
                    } else {
                        ops.putDatasource(JsonUtil.from(text, DatasourceConfig.class));
                    }
                    ops.commit();
                }
            }
        });
        store.addChangedCallback(new ChangedCallback() {
            @Override
            public String getInterestedPath() {
                return "clusters";
            }

            @Override
            public void onChanged(String path, String text, boolean delete) throws Exception  {
                String clusterName = endPath(path);
                try (MycatRouterConfigOps ops = ConfigUpdater.getOps(CoordinatorMetadataStorageManager.this)) {
                    if (delete) {
                        ops.removeReplica(clusterName);
                    } else {
                        ops.putReplica(JsonUtil.from(text, ClusterConfig.class));
                    }
                    ops.commit();
                }
            }
        });
        store.addChangedCallback(new ChangedCallback() {
            @Override
            public String getInterestedPath() {
                return "sequences";
            }

            @Override
            public void onChanged(String path, String text, boolean delete) throws Exception  {
                String sequenceName = endPath(path);
                try (MycatRouterConfigOps ops = ConfigUpdater.getOps(CoordinatorMetadataStorageManager.this)) {
                    if (delete) {
                        ops.removeSequence(sequenceName);
                    } else {
                        ops.putSequence(JsonUtil.from(text, SequenceConfig .class));
                    }
                    ops.commit();
                }
            }
        });
        store.addChangedCallback(new ChangedCallback() {
            @Override
            public String getInterestedPath() {
                return "users";
            }

            @Override
            public void onChanged(String path, String text, boolean delete) throws Exception {
                String username = endPath(path);
                try (MycatRouterConfigOps ops = ConfigUpdater.getOps(CoordinatorMetadataStorageManager.this)) {
                    if (delete) {
                        ops.deleteUser(username);
                    } else {
                        ops.putUser(JsonUtil.from(text, UserConfig .class));
                    }
                    ops.commit();
                }
            }
        });
    }

    final Store store;
    final FileMetadataStorageManager.State state = new FileMetadataStorageManager.State();
    final ConfigReaderWriter readerWriter;
    final String datasourceProvider;

    @Override
    void start() throws Exception {
        try (ConfigOps configOps = startOps()) {
            configOps.commit(new MycatRouterConfigOps(loadFromLocalConfigCenter(), configOps));
        }
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
                            .toMap(k -> k.getUniqueName(), v -> readerWriter.transformation(v))));
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

        MycatRouterConfig routerConfig = new MycatRouterConfig();
        routerConfig.setSchemas(logicSchemaConfigs);
        routerConfig.setClusters(clusterConfigs);
        routerConfig.setDatasources(datasourceConfigs);
        routerConfig.setPrototype(prototype);
        routerConfig.setUsers(userConfigs);
        routerConfig.setSequences(sequenceList);
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

                if (routerConfig.isUpdateSchemas()) {
                    store.set("schemas",
                            mycatRouterConfig
                                    .getSchemas()
                                    .stream()
                                    .collect(Collectors
                                            .toMap(k -> k.getSchemaName(), v -> readerWriter.transformation(v))));
                }
                if (routerConfig.isUpdateClusters()) {
                    store.set("clusters",
                            mycatRouterConfig
                                    .getClusters()
                                    .stream()
                                    .collect(Collectors
                                            .toMap(k -> k.getName(), v -> readerWriter.transformation(v))));
                }
                if (routerConfig.isUpdateDatasources()) {
                    store.set("datasources",
                            mycatRouterConfig
                                    .getDatasources()
                                    .stream()
                                    .collect(Collectors
                                            .toMap(k -> k.getName(), v -> readerWriter.transformation(v))));
                }
                if (routerConfig.isUpdateUsers()) {
                    store.set("users",
                            mycatRouterConfig
                                    .getUsers()
                                    .stream()
                                    .collect(Collectors
                                            .toMap(k -> k.getUsername(), v -> readerWriter.transformation(v))));

                }
                if (routerConfig.isUpdateSequences()) {
                    store.set("sequences",
                            mycatRouterConfig
                                    .getSequences()
                                    .stream()
                                    .collect(Collectors
                                            .toMap(k -> k.getUniqueName(), v -> readerWriter.transformation(v))));
                }
//                if (routerConfig.isUpdatePrototype()) {
//                    store.set("prototype",
//                            mycatRouterConfig.getPrototype()
//                    );
//                }


                state.configTimestamp = LocalDateTime.now().toString();
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

        void addChangedCallback(ChangedCallback changedCallback);

        void begin();

        String get(String schema);

        void set(String schemas, String transformation);

        void set(String schemas, Map<String, String> transformation);

        Map<String, String> getMap(String schemas);

        void commit();

        void close();
    }

    interface ChangedCallback {
        String getInterestedPath();

        void onChanged(String path, String text, boolean delete) throws Exception;

        default String endPath(String path) {
            return path.substring(path.indexOf('/') + 1);
        }
    }
}
