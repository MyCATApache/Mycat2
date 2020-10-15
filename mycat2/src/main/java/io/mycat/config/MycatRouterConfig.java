package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode
@Data
public class MycatRouterConfig {
    public ShardingQueryRootConfig shardingQueryRootConfig = new ShardingQueryRootConfig();
    public ClusterRootConfig cluster = new ClusterRootConfig();
    public List<DatasourceConfig> datasources = new ArrayList<>();
    public List<UserConfig> users =  new ArrayList<>();
    public List<SequenceConfig> sequences =  new ArrayList<>();
}