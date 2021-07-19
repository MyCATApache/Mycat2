package io.mycat.ui;

import com.google.common.collect.ImmutableMap;
import io.mycat.MetaClusterCurrent;
import io.mycat.config.*;
import io.mycat.util.NameMap;

import java.util.List;
import java.util.Optional;

public class InfoProviderImpl implements InfoProvider {
    @Override
    public List<LogicSchemaConfig> schemas() {
        MycatRouterConfig mycatRouterConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        return mycatRouterConfig.getSchemas();
    }

    @Override
    public List<ClusterConfig> clusters() {
        MycatRouterConfig mycatRouterConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        return mycatRouterConfig.getClusters();
    }

    @Override
    public List<DatasourceConfig> datasources() {
        MycatRouterConfig mycatRouterConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        return mycatRouterConfig.getDatasources();
    }

    @Override
    public Optional<LogicSchemaConfig> getSchemaConfigByName(String schemaName) {
        MycatRouterConfig mycatRouterConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        for (LogicSchemaConfig schema : mycatRouterConfig.getSchemas()) {
            if(schema.getSchemaName().equals(schemaName)){
             return Optional.ofNullable(schema);
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<DatasourceConfig> getDatasourceConfigByPath(String path) {
        return Optional.ofNullable(new DatasourceConfig());
    }

    @Override
    public Optional<ClusterConfig> getClusterConfigByPath(String path) {
        return Optional.ofNullable(new ClusterConfig());
    }

    @Override
    public String translate(String name) {
        return map.get(name, false);
    }

    NameMap<String> map = NameMap.immutableCopyOf((ImmutableMap)
            ImmutableMap.builder()
                    .put("schemaName", "库名")
                    .put("defaultTargetName", "默认映射库目标")
                    .build());

}
