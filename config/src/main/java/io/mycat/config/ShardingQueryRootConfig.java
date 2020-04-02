package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class ShardingQueryRootConfig {
    Map<String, LogicSchemaConfig> schemas = new HashMap<>();
    PrototypeServer prototype;

    public Map<String, LogicSchemaConfig> getSchemas() {
        return schemas == null?Collections.emptyMap():schemas;
    }

    @AllArgsConstructor
    @Data
    @Builder
    public static class BackEndTableInfoConfig {
        private String targetName;
        private String schemaName;
        private String tableName;

        public BackEndTableInfoConfig() {
        }
    }


    @Data
    public static final class LogicSchemaConfig {
        Map<String, ShardingTableConfig> shadingTables = new HashMap<>();
        Map<String, GlobalTableConfig> globalTables = new HashMap<>();
    }

    @AllArgsConstructor
    @Data
    @Builder
    public static final class Column {
        String columnName;
        SharingFuntionRootConfig.ShardingFuntion function;
        String shardingType;
        List<String> map;

        public Column() {
        }

        public List<String> getMap() {
            return map == null ? Collections.emptyList() : map;
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PrototypeServer {
        String url = "jdbc:mysql://127.0.0.1:3306?useUnicode=true&serverTimezone=UTC";
        String user = "root";
        String password = "123456";
        String targetName = "defaultDs";
    }

}