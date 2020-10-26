package io.mycat;

import io.mycat.config.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


public class MycatRouterConfigOps implements AutoCloseable{
    private final MycatRouterConfig mycatRouterConfig;
    private final ConfigOps configOps;
    List<LogicSchemaConfig> schemas = null;
    List<ClusterConfig> clusters = null;
    List<UserConfig> users = null;
    List<SequenceConfig> sequences = null;
    List<DatasourceConfig> datasources = null;
    String prototype = null;

    public boolean isUpdateSchemas(){
        return schemas!=null;
    }
    public boolean isUpdateClusters(){
        return clusters!=null;
    }
    public boolean isUpdateUsers(){
        return users!=null;
    }
    public boolean isUpdateSequences(){
        return sequences!=null;
    }
    public boolean isUpdateDatasources(){
        return datasources!=null;
    }
    public boolean isUpdatePrototype(){
        return prototype!=null;
    }
    public MycatRouterConfigOps(
            MycatRouterConfig mycatRouterConfig,
            ConfigOps configOps
            ) {
        this.mycatRouterConfig = mycatRouterConfig;
        this.prototype =mycatRouterConfig.getPrototype();
        this.configOps = configOps;
    }


    public void addSchema(String schemaName, String targetName) {
        this.schemas = mycatRouterConfig.getSchemas();
        List<LogicSchemaConfig> schemas = this.schemas;
        LogicSchemaConfig schemaConfig;
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> schemaName.equals(i.getSchemaName())).findFirst();
        first.ifPresent(i->{
            i.setTargetName(targetName);
        });
        schemas.add(schemaConfig = new LogicSchemaConfig());
        schemaConfig.setSchemaName(schemaName);
    }


    public void addTargetOnExistedSchema(String schemaName, String targetName) {
        this.schemas = mycatRouterConfig.getSchemas();
        List<LogicSchemaConfig> schemas =  this.schemas;
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> i.getSchemaName().equals(schemaName)).findFirst();
        first.ifPresent(i -> i.setTargetName(targetName));
    }


    public void dropSchema(String schemaName) {
        this.schemas = mycatRouterConfig.getSchemas();
        List<LogicSchemaConfig> schemas =  this.schemas;
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> i.getSchemaName().equals(schemaName)).findFirst();
        first.ifPresent(o ->{schemas.remove(o);});
    }


    public void putNormalTable(String schemaName, String tableName, String sqlString) {
        String defaultTarget = "prototype";
        putNormalTable(schemaName, tableName, sqlString, defaultTarget);
    }


    public void putNormalTable(String schemaName, String tableName, String sqlString, String targetName) {
        this.schemas = mycatRouterConfig.getSchemas();
        List<LogicSchemaConfig> schemas =  this.schemas;
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> i.getSchemaName().equals(schemaName)).findFirst();
        first.ifPresent(logicSchemaConfig -> {
            Map<String, NormalTableConfig> normalTables = logicSchemaConfig.getNormalTables();
            NormalTableConfig normalTableConfig = new NormalTableConfig();
            normalTableConfig.setCreateTableSQL(sqlString.toString());
            normalTableConfig.setDataNode(NormalBackEndTableInfoConfig.builder()
                    .targetName(targetName)
                    .schemaName(schemaName)
                    .tableName(tableName)
                    .build());
            normalTables.put(tableName, normalTableConfig);
        });
    }


    public void putGlobalTable(String schemaName, String tableName, String sqlString) {
        this.schemas = mycatRouterConfig.getSchemas();
        List<LogicSchemaConfig> schemas =  this.schemas;
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> i.getSchemaName().equals(schemaName)).findFirst();
        first.ifPresent(logicSchemaConfig -> {
            Map<String, GlobalTableConfig> normalTables = logicSchemaConfig.getGlobalTables();
            GlobalTableConfig globalTableConfig = new GlobalTableConfig();
            List<ClusterConfig> clusters = mycatRouterConfig.getClusters();
            List<String> allReplica = clusters.stream().map(i -> i.getName()).collect(Collectors.toList());
            globalTableConfig.setCreateTableSQL(sqlString.toString());
            globalTableConfig.setDataNodes(allReplica.stream()
                    .map(i -> {
                        GlobalBackEndTableInfoConfig backEndTableInfoConfig = new GlobalBackEndTableInfoConfig();
                        backEndTableInfoConfig.setTargetName(i);
                        return backEndTableInfoConfig;
                    }).collect(Collectors.toList()));
            normalTables.put(tableName, globalTableConfig);
        });
    }


    public void removeTable(String schemaName, String tableName) {
        this.schemas = mycatRouterConfig.getSchemas();
        List<LogicSchemaConfig> schemas =  this.schemas;
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> i.getSchemaName().equals(schemaName)).findFirst();
        first.ifPresent(logicSchemaConfig -> {
            logicSchemaConfig.getNormalTables().remove(tableName);
            logicSchemaConfig.getGlobalTables().remove(tableName);
            logicSchemaConfig.getShadingTables().remove(tableName);
            logicSchemaConfig.getCustomTables().remove(tableName);
        });
    }


    public void putShardingTable(String schemaName, String tableName, String tableStatement, Map<String, Object> infos) {
        Map<String, String> ranges = (Map) infos.get("ranges");
        Map<String, String> dataNodes = (Map) infos.get("dataNodes");
        Map<String, String> properties = (Map) infos.get("properties");
        ShardingTableConfig.ShardingTableConfigBuilder builder = ShardingTableConfig.builder();
        ShardingTableConfig config = builder
                .createTableSQL(tableStatement.toString())
                .function(ShardingFuntion.builder().properties(properties).ranges(ranges).build())
                .dataNode(ShardingBackEndTableInfoConfig
                        .builder()
                        .schemaNames(dataNodes.get("schemaNames"))
                        .tableNames(dataNodes.get("tableNames"))
                        .targetNames(dataNodes.get("targetNames")).build())
                .build();

        //todo check  ShardingTableConfig right
        this.schemas = mycatRouterConfig.getSchemas();
        List<LogicSchemaConfig> schemas =  this.schemas;
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> i.getSchemaName().equals(schemaName)).findFirst();
        first.ifPresent(logicSchemaConfig -> {
            Map<String, ShardingTableConfig> shadingTables = logicSchemaConfig.getShadingTables();
            shadingTables.put(tableName, config);
        });
    }


    public void putUser(String username, String password, String ip, String transactionType) {
        this.users = mycatRouterConfig.getUsers();
        users.add(UserConfig.builder()
                .username(username)
                .password(password)
                .ip(ip)
                .transactionType(transactionType)
                .build());
    }


    public void remove(String username) {
        this.users = mycatRouterConfig.getUsers();
        users.stream().filter(i -> username.equals(i.getUsername()))
                .findFirst().ifPresent(i -> users.remove(i));
    }


    public void putSequence(String name, String clazz, String args) {
        this.sequences = mycatRouterConfig.getSequences();
        sequences.add(new SequenceConfig(name, clazz, args));
    }


    public void removeSequence(String name) {
        this.sequences = mycatRouterConfig.getSequences();
        sequences.stream()
                .filter(i -> name.equals(i.getName())).findFirst()
                .ifPresent(i -> sequences.remove(i));
    }



    public void putDatasource(DatasourceConfig datasourceConfig) {
        this.datasources = mycatRouterConfig.getDatasources();
        Optional<DatasourceConfig> first = datasources.stream().filter(i -> datasourceConfig.getName().equals(i.getName())).findFirst();
        if (first.isPresent()) {
            datasources.remove(first.get());
        } else {
            datasources.add(datasourceConfig);
        }
    }

    public void removeDatasource(String datasourceName) {
        this.datasources = mycatRouterConfig.getDatasources();
        Optional<DatasourceConfig> first = datasources.stream().filter(i -> datasourceName.equals(i.getName())).findFirst();
        first.ifPresent(datasources::remove);
    }

    public void putReplica(ClusterConfig clusterConfig) {
        this.clusters = mycatRouterConfig.getClusters();
        Optional<ClusterConfig> first = this.clusters.stream().filter(i -> clusterConfig.getName().equals(i.getName())).findFirst();
        first.ifPresent(clusters::remove);
        clusters.add(clusterConfig);
    }

    public void removeReplica(String replicaName) {
        this.clusters = mycatRouterConfig.getClusters();
        List<ClusterConfig> clusters = this.clusters;
        Optional<ClusterConfig> first = clusters.stream().filter(i -> replicaName.equals(i.getName())).findFirst();
        first.ifPresent(clusters::remove);
    }


    public List<ClusterConfig> getClusters() {
        return clusters;
    }

    public List<UserConfig> getUsers() {
        return users;
    }

    public List<SequenceConfig> getSequences() {
        return sequences;
    }

    public List<DatasourceConfig> getDatasources() {
        return datasources;
    }

    public MycatRouterConfig getMycatRouterConfig() {
        return mycatRouterConfig;
    }

    public List<LogicSchemaConfig> getSchemas() {
        return schemas;
    }

    public String getPrototype() {
        return prototype;
    }


    public MycatRouterConfig currentConfig() {
        return mycatRouterConfig;
    }


    public void commit() {
        this.configOps.commit(this);
    }

    public void close() {
        this.configOps.close();
    }
}
