package io.mycat.config.shardingQuery;

import io.mycat.config.ConfigurableRoot;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ShardingQueryRootConfig extends ConfigurableRoot {
    final Map<String, Map<String, List<BackEndTableInfo>>> schemaBackendMetaMap = new ConcurrentHashMap<>();

    static class LogicTable{
        List<BackEndTableInfo> backEndTableInfos;

    }
    static class BackEndTableInfo{
        private String hostName;
        private String schemaName;
        private String tableName;
    }
}