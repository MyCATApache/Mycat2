package io.mycat.router.mycat1xfunction;

import io.mycat.router.ShardingTableHandler;
import io.mycat.router.util.PartitionUtil;

import java.util.Map;

public class SimplePartitionByLong extends PartitionByLong {
    @Override
    public void init(ShardingTableHandler table, Map<String, String> properties, Map<String, String> ranges) {
        int count = Integer.parseInt(properties.get("partitionCount"));
        properties.put("partitionLength", String.valueOf(PartitionUtil.PARTITION_LENGTH / count));
        super.init(table,properties, ranges);
    }

    @Override
    public String name() {
        return "SimplePartitionByLong";
    }
}