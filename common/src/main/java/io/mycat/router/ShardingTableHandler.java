package io.mycat.router;

import io.mycat.DataNode;
import io.mycat.SimpleColumnInfo;
import io.mycat.TableHandler;

import java.util.List;

public interface ShardingTableHandler extends TableHandler {

    CustomRuleFunction function();

    List<DataNode> getShardingBackends();

    List<SimpleColumnInfo> getColumns();
}
