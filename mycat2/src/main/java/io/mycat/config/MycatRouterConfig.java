package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode
@Data
public class MycatRouterConfig {
    public List<LogicSchemaConfig> schemas = new ArrayList<>();// schemas/xxx.schema.yml
    public List<ClusterConfig> clusters = new ArrayList<>();// clusters/xxx.cluster.yml
    public List<DatasourceConfig> datasources = new ArrayList<>();// datasources/xxx.datasource.yml
    public List<UserConfig> users = new ArrayList<>();// users/xxx.user.yml
    public List<SequenceConfig> sequences = new ArrayList<>();// sequences/xxx.sequence.yml
    public String prototype = "prototype";// mycat.yml
    public List<String> serverList = new ArrayList<>();
}