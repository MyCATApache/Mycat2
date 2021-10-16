package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@EqualsAndHashCode
@Data
public class MycatRouterConfig {
    public List<LogicSchemaConfig> schemas = new ArrayList<>();// schemas/xxx.schema.yml
    public List<ClusterConfig> clusters = new ArrayList<>();// clusters/xxx.cluster.yml
    public List<DatasourceConfig> datasources = new ArrayList<>();// datasources/xxx.datasource.yml
    public List<UserConfig> users = new ArrayList<>();// users/xxx.user.yml
    public List<SequenceConfig> sequences = new ArrayList<>();// sequences/xxx.sequence.yml
    public List<SqlCacheConfig> sqlCacheConfigs = new ArrayList<>();
    public String prototype = "prototype";// mycat.yml

    public boolean baseMatch(MycatRouterConfig config){

        return match((List)this.getSchemas(), (List) config.getSchemas())
                &&match((List)this.getClusters(), (List) config.getClusters())
                &&match((List)this.getDatasources(), (List) config.getDatasources())
                &&match((List)this.getUsers(), (List) config.getUsers())
                &&match((List)this.getSequences(), (List) config.getSequences())
                &&match((List)this.getSqlCacheConfigs(), (List) config.getSqlCacheConfigs());
    }

    private boolean match(List<KVObject> lKvObjects, List<KVObject> rKvObjects) {
        Set<String> leftSets = lKvObjects.stream().map(i -> i.keyName()).collect(Collectors.toSet());
        Set<String> rightSets = rKvObjects.stream().map(i -> i.keyName()).collect(Collectors.toSet());

        return leftSets.equals(rightSets);
    }

    public boolean containsPrototypeTargetName() {
        return hasClusterPrototypeName()
                ||
                hasDatasourcePrototypeName();
    }

    public boolean hasClusterPrototypeName() {
        return clusters.stream().anyMatch(i -> prototype.equalsIgnoreCase(i.getName()));
    }

    public boolean hasDatasourcePrototypeName() {
        return datasources.stream().anyMatch(i -> prototype.equalsIgnoreCase(i.getName()));
    }

    public boolean hasDatasourcePrototypeDsName() {
        return datasources.stream().anyMatch(i -> "prototypeDs".equalsIgnoreCase(i.getName()));
    }

    public void fixPrototypeTargetName() {
        if (containsPrototypeTargetName()) {
            return;
        }
        if (hasDatasourcePrototypeDsName()){
            ClusterConfig clusterConfig = new ClusterConfig();
            clusterConfig.setName(prototype);
            List<String> masters = clusterConfig.getMasters();
            masters.add("prototypeDs");
            clusters.add(clusterConfig);
        }

    }
}