package io.mycat.router;

import io.mycat.BackendTableInfo;
import io.mycat.DataNode;
import io.mycat.TableHandler;
import io.mycat.SimpleColumnInfo;

import java.util.List;

public interface ShardingTableHandler extends TableHandler {

    public boolean isNatureTable();

    public List<DataNode> getShardingBackends();

    SimpleColumnInfo.ShardingInfo getNatureTableColumnInfo();

    SimpleColumnInfo.ShardingInfo getReplicaColumnInfo();

    SimpleColumnInfo.ShardingInfo getDatabaseColumnInfo();

    SimpleColumnInfo.ShardingInfo getTableColumnInfo();

    List<SimpleColumnInfo> getColumns();
}
