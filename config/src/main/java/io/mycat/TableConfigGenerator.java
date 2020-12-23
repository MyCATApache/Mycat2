package io.mycat;

import io.mycat.config.GlobalTableConfig;
import io.mycat.config.LogicSchemaConfig;
import io.mycat.config.NormalTableConfig;
import io.mycat.config.ShardingTableConfig;

import java.util.List;
import java.util.Map;

public abstract class TableConfigGenerator {
    public final MycatConfig config;
    public final LogicSchemaConfig schemaConfig;
    public final List<String> listOptions;
    public final Map<String, String> kvOptions;

    public TableConfigGenerator(MycatConfig config, LogicSchemaConfig schemaConfig, List<String> listOptions, Map<String, String> kvOptions) {
        this.config = config;
        this.schemaConfig = schemaConfig;
        this.listOptions = listOptions;
        this.kvOptions = kvOptions;
    }

    abstract Map<String, ShardingTableConfig> generateShardingTable();

    abstract Map<String, GlobalTableConfig> generateGlobalTable();

    abstract Map<String, NormalTableConfig> generateNormalTable();
}