package io.mycat.metadata;

import io.mycat.BackendTableInfo;
import io.mycat.TableHandler;

import java.util.Map;
import java.util.Set;

public interface GlobalTableHandler extends TableHandler {
    public BackendTableInfo getGlobalBackendTableInfoForQuery(boolean update);

    public BackendTableInfo getMycatGlobalPhysicalBackendTableInfo(Set<String> context);

    public Map<String, BackendTableInfo> getDataNodeMap();
}