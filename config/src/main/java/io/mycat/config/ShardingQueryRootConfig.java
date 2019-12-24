package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.*;


public class ShardingQueryRootConfig {
    List<LogicSchemaConfig> schemas = new ArrayList<>();
    public void setSchemas(List<LogicSchemaConfig> schemas) {
        this.schemas = schemas;
    }

    @AllArgsConstructor
    @Data
    public static class LogicTableConfig {
        String tableName = "";
        List<BackEndTableInfoConfig> queryPhysicalTable = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        String function = "";
        Map<String, String> properties = new HashMap<>();
        Map<String, String> ranges = new HashMap<>();
        String createTableSQL;

        public LogicTableConfig() {
        }
    }

    @AllArgsConstructor
    @Data
    public static class BackEndTableInfoConfig {
        private String dataNodeName;
        private String replicaName;
        private String hostName;
        private String schemaName;
        private String tableName;

        public BackEndTableInfoConfig() {
        }
    }

    public List<LogicSchemaConfig> getSchemas() {
        return schemas;
    }

    @AllArgsConstructor
    @Data
    public static final class LogicSchemaConfig {
        String schemaName = "";
        List<LogicTableConfig> tables = new ArrayList<>();

        public LogicSchemaConfig() {
        }

    }
}