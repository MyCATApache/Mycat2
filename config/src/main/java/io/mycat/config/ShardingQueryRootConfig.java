package io.mycat.config;

import io.mycat.util.YamlUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;

@Data
public class ShardingQueryRootConfig {
    List<LogicSchemaConfig> schemas = new ArrayList<>();
    PrototypeServer prototype;

    public List<LogicSchemaConfig> getSchemas() {
        return schemas;
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
        String schemaName;
        String targetName;
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
        String targetName = "defaultDs";
    }

    public static void main(String[] args) {
        ShardingQueryRootConfig rootConfig = new ShardingQueryRootConfig();
        LogicSchemaConfig logicSchemaConfig = new LogicSchemaConfig();
        logicSchemaConfig.setTargetName("db1");
        logicSchemaConfig.setTargetName("defaultDs");
        rootConfig.getSchemas().add(logicSchemaConfig);
        System.out.println(YamlUtil.dump(rootConfig));
    }

}