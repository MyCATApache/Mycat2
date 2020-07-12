package io.mycat.hbt3;

import io.mycat.hbt4.ShardingInfo;

import java.util.stream.IntStream;


public class RangePartInfo implements PartInfo {
    private final int startIndex;
    private final int endIndex;
    private final ShardingInfo shardingInfo;


    public RangePartInfo(int startIndex, int endIndex, ShardingInfo shardingInfo) {
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.shardingInfo = shardingInfo;
    }

    @Override
    public int size() {
        return endIndex - startIndex;
    }

    @Override
    public Part getPart(int index) {
        int schemaSize = shardingInfo.getSchemaSize();
        int tableSize = shardingInfo.getTableSize();

        int count = 0;
        for (int i = 0; i < schemaSize; i++) {
            for (int j = 0; j < tableSize; j++) {
                if (count == index) {
                    return new PartImpl(shardingInfo.getDatasourceSize(), i, j);
                }
                count++;
            }
        }
        throw new IllegalArgumentException();
    }

    @Override
    public Part[] toPartArray() {
        return IntStream.range(startIndex, endIndex).mapToObj(i -> getPart(i)).toArray(i -> new Part[i]);
    }

    @Override
    public String toString() {
        return "[" +
                startIndex + "-" + endIndex + "]";
    }
}