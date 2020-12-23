package io.mycat.calcite.table;

import io.mycat.BackendTableInfo;
import io.mycat.DataNode;
import io.mycat.TableHandler;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface GlobalTableHandler extends TableHandler {
    public List<DataNode> getGlobalDataNode();
}