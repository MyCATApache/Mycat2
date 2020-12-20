package io.mycat.router;

import io.mycat.DataNode;
import io.mycat.SimpleColumnInfo;
import io.mycat.TableHandler;

import java.util.List;
import java.util.Optional;

public interface ShardingTableHandler extends TableHandler {

    CustomRuleFunction function();

    List<DataNode> dataNodes();

    @Override
    List<SimpleColumnInfo> getColumns();


    Optional<Iterable<Object[]>> canIndexTableScan(int[] projects);

    Optional<Iterable<Object[]>> canIndexTableScan(int[] projects, int[] filterIndex,Object[] value);

    Optional<Iterable<Object[]>> canIndexTableScan();

    boolean canIndex();

    public int getIndexBColumnName(String name);
}
