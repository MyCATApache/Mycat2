package io.mycat.router.function;

import io.mycat.router.util.PartitionUtil;

import java.util.Map;

public class SimplePartitionByLong extends PartitionByLong {
    @Override
    public void init(Map<String, String> properties, Map<String, String> ranges) {
        int count = Integer.parseInt(properties.get("partitionCount"));
        properties.put("partitionLength", String.valueOf(PartitionUtil.PARTITION_LENGTH / count));
        super.init(properties, ranges);
    }

    @Override
    public String name() {
        return "SimplePartitionByLong";
    }
}