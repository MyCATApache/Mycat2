package io.mycat;

import io.mycat.config.DatasourceConfig;

import java.util.Map;

public class ProxyDatasourceConfigProvider implements DatasourceConfigProvider {
    @Override
    public Map<String, DatasourceConfig> get() {
        DatasourceConfigProvider datasourceConfigProvider = MetaClusterCurrent.wrapper(DatasourceConfigProvider.class);
        return datasourceConfigProvider.get();
    }
}