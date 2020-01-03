package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.*;

@Data
public class ShardingQueryRootConfig {
    Map<String, LogicSchemaConfig> schemas = new HashMap<>();
    Map<String,List<BackEndTableInfoConfig>> dataNodes = new HashMap<>();
    PrototypeServer prototype;

    @AllArgsConstructor
    @Data
    @Builder
    public static class LogicTableConfig {
        String dataNodeName;
        List<Column> columns = new ArrayList<>();
        String createTableSQL;
    }

    @AllArgsConstructor
    @Data
    @Builder
    public static class BackEndTableInfoConfig {
        private String replicaName;
        private String schemaName;
        private String tableName;
    }


    @Data
    public static final class LogicSchemaConfig {
        Map<String,LogicTableConfig> tables = new HashMap<>();
    }

    @AllArgsConstructor
    @Data
    @Builder
    public static final class Column {
        String columnName;
        SharingFuntionRootConfig.ShardingFuntion function;
        final String shardingType;
        final List<String> map;

        public List<String> getMap() {
            return map == null? Collections.emptyList():map;
        }
    }

    @Data
    @Builder
    public static class PrototypeServer{
        String url;
        String user;
        String password;
    }

}