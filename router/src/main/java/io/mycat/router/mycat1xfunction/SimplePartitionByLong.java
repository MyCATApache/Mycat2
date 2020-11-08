package io.mycat.router.mycat1xfunction;

import io.mycat.router.ShardingTableHandler;
import io.mycat.router.util.PartitionUtil;

import java.util.Map;
import java.util.Objects;

public class SimplePartitionByLong extends PartitionByLong {
    @Override
    public void init(ShardingTableHandler table, Map<String, Object> properties, Map<String, Object> ranges) {
        int count = Integer.parseInt(Objects.toString(properties.get("partitionCount")));
        properties.put("partitionLength", String.valueOf(PartitionUtil.PARTITION_LENGTH / count));
        super.init(table,properties, ranges);
    }

    @Override
    public String name() {
        return "SimplePartitionByLong";
    }
}