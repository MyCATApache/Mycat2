package io.mycat.config.shardingQuery;

import io.mycat.config.ConfigurableRoot;
import lombok.AllArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ShardingQueryRootConfig extends ConfigurableRoot {

    public void setSchemas(Map<String, Map<String, LogicTableConfig>> schemas) {
        this.schemas = schemas;
    }

    Map<String, Map<String, LogicTableConfig>> schemas = new HashMap<>();

    @AllArgsConstructor
    public static class LogicTableConfig {
        List<BackEndTableInfoConfig> physicalTable;
        List<String> columns;
        String function;
        Map<String, String> properties;
        Map<String, String> ranges;

        public LogicTableConfig() {
        }

        public List<BackEndTableInfoConfig> getPhysicalTable() {
            return physicalTable;
        }


        public void setPhysicalTable(List<BackEndTableInfoConfig> physicalTable) {
            this.physicalTable = physicalTable;
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

    public Map<String, Map<String, LogicTableConfig>> getSchemas() {
        return schemas;
    }
}