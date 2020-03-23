package io.mycat.metadata;

import io.mycat.BackendTableInfo;
import io.mycat.queryCondition.SimpleColumnInfo;

import java.util.List;

public interface ShardingTableHandler extends TableHandler {

    public boolean isNatureTable();

    public List<BackendTableInfo> getShardingBackends();

    SimpleColumnInfo.ShardingInfo getNatureTableColumnInfo();

    SimpleColumnInfo.ShardingInfo getReplicaColumnInfo();

    SimpleColumnInfo.ShardingInfo getDatabaseColumnInfo();

    SimpleColumnInfo.ShardingInfo getTableColumnInfo();

    List<SimpleColumnInfo> getRawColumns();
}
