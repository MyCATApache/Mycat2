package io.mycat.config.shardingQuery;

import io.mycat.config.ConfigurableRoot;
import lombok.AllArgsConstructor;

import java.util.*;


public class ShardingQueryRootConfig extends ConfigurableRoot {
    List<LogicSchemaConfig> schemas = new ArrayList<>();
    public void setSchemas(List<LogicSchemaConfig> schemas) {
        this.schemas = schemas;
    }



    @AllArgsConstructor
    public static class LogicTableConfig {
        String tableName = "";
        List<BackEndTableInfoConfig> queryPhysicalTable = Collections.emptyList();
        List<String> columns = Collections.emptyList();
        String function = "";
        Map<String, String> properties = new HashMap<>();
        Map<String, String> ranges = new HashMap<>();

        public LogicTableConfig() {
        }

        public List<BackEndTableInfoConfig> getQueryPhysicalTable() {
            return queryPhysicalTable;
        }


        public void setQueryPhysicalTable(List<BackEndTableInfoConfig> queryPhysicalTable) {
            this.queryPhysicalTable = queryPhysicalTable;
        }

        public String getFunction() {
            return function;
        }


        public void setFunction(String function) {
            this.function = function;
        }


        public Map<String, String> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }


        public Map<String, String> getRanges() {
            return ranges;
        }


        public void setRanges(Map<String, String> ranges) {
            this.ranges = ranges;
        }


        public List<String> getColumns() {
            return columns;
        }

        public void setColumns(List<String> columns) {
            this.columns = columns;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }
    }

    @AllArgsConstructor
    public static class BackEndTableInfoConfig {
        private String dataNodeName;
        private String replicaName;
        private String hostName;
        private String schemaName;
        private String tableName;

        public BackEndTableInfoConfig() {
        }

        public String getDataNodeName() {
            return dataNodeName;
        }

        public void setDataNodeName(String dataNodeName) {
            this.dataNodeName = dataNodeName;
        }

        public String getReplicaName() {
            return replicaName;
        }

        public void setReplicaName(String replicaName) {
            this.replicaName = replicaName;
        }

        public String getHostName() {
            return hostName;
        }

        public void setHostName(String hostName) {
            this.hostName = hostName;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

    }

    public List<LogicSchemaConfig> getSchemas() {
        return schemas;
    }

    @AllArgsConstructor
    public static final class LogicSchemaConfig {
        String schemaName = "";
        List<LogicTableConfig> tables = Collections.emptyList();

        public LogicSchemaConfig() {
        }

        public String getSchemaName() {
            return schemaName;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }

        public List<LogicTableConfig> getTables() {
            return tables;
        }

        public void setTables(List<LogicTableConfig> tables) {
            this.tables = tables;
        }
    }

}