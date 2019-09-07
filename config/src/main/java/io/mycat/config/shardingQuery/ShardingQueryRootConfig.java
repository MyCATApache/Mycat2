package io.mycat.config.shardingQuery;

import io.mycat.config.ConfigurableRoot;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ShardingQueryRootConfig extends ConfigurableRoot {
    final Map<String, Map<String, List<BackEndTableInfo>>> schemaBackendMetaMap = new ConcurrentHashMap<>();

    public static class LogicTable {
        List<BackEndTableInfo> backEndTableInfos;
        String function;
        Map<String, String> properties;
        Map<String, String> ranges;

        public List<BackEndTableInfo> getBackEndTableInfos() {
            return backEndTableInfos;
        }

        public void setBackEndTableInfos(List<BackEndTableInfo> backEndTableInfos) {
            this.backEndTableInfos = backEndTableInfos;
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
    }

    public static class BackEndTableInfo {
        private String dataNodeName;
        private String replicaName;
        private String hostName;
        private String schemaName;
        private String tableName;

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
    }
}