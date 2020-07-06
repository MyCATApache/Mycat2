package io.mycat.querycondition;

import io.mycat.BackendTableInfo;
import io.mycat.router.ShardingTableHandler;

import java.util.List;

public interface DataMapping {
    void assignment(boolean b, String s1, String value);

    List<BackendTableInfo> calculate(ShardingTableHandler logicTable);

    void assignmentRange(boolean or, String columnName, String startValue, String endValue);

    void merge(DataMapping dataMappingRule);
}
