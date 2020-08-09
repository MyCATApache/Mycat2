package io.mycat.metadata;

import io.mycat.DataNode;
import io.mycat.TableHandler;

public interface NormalTableHandler extends TableHandler {
    public DataNode getDataNode();
}