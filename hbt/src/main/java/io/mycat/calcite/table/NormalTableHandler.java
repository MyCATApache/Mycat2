package io.mycat.calcite.table;

import io.mycat.DataNode;
import io.mycat.TableHandler;

public interface NormalTableHandler extends TableHandler {
    public DataNode getDataNode();
}