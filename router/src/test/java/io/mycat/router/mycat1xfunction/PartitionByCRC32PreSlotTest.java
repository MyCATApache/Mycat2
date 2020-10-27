package io.mycat.router.mycat1xfunction;

import io.mycat.router.ShardingTableHandler;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class PartitionByCRC32PreSlotTest {

    @Test
    public void test() {
        PartitionByCRC32PreSlot partition = getPartitionByCRC32PreSlot(1000);
        Assert.assertEquals(521, partition.calculateIndex("1000316"));
        Assert.assertEquals(637, partition.calculateIndex("2"));
    }

    @Test
    public void test2() {
        PartitionByCRC32PreSlot partition = getPartitionByCRC32PreSlot(2);


        Assert.assertEquals(0, partition.calculateIndex("1"));
        Assert.assertEquals(1, partition.calculateIndex("2"));
        Assert.assertEquals(0, partition.calculateIndex("3"));
        Assert.assertEquals(1, partition.calculateIndex("4"));
        Assert.assertEquals(0, partition.calculateIndex("5"));
        Assert.assertEquals(0, partition.calculateIndex("6"));
        Assert.assertEquals(0, partition.calculateIndex("7"));
        Assert.assertEquals(0, partition.calculateIndex("8"));
        Assert.assertEquals(0, partition.calculateIndex("9"));

        Assert.assertEquals(0, partition.calculateIndex("9999"));
        Assert.assertEquals(1, partition.calculateIndex("123456789"));
        Assert.assertEquals(1, partition.calculateIndex("35565"));

    }

    @Test
    public void test3() {
        PartitionByCRC32PreSlot partition = getPartitionByCRC32PreSlot(3);


        Assert.assertEquals(1 , partition.calculateIndex("1"));
        Assert.assertEquals( 1 , partition.calculateIndex("2"));
        Assert.assertEquals( 0 , partition.calculateIndex("3"));
        Assert.assertEquals(2 , partition.calculateIndex("4"));
        Assert.assertEquals(0 , partition.calculateIndex("5"));
        Assert.assertEquals(1 , partition.calculateIndex("6"));
        Assert.assertEquals(1 , partition.calculateIndex("7"));
        Assert.assertEquals( 0 , partition.calculateIndex("8"));
        Assert.assertEquals(0 , partition.calculateIndex("9"));

        Assert.assertEquals(0 , partition.calculateIndex("9999"));
        Assert.assertEquals(2 , partition.calculateIndex("123456789"));
        Assert.assertEquals(2 , partition.calculateIndex("35565"));

    }

    private PartitionByCRC32PreSlot getPartitionByCRC32PreSlot(int count) {
        String text = String.valueOf(count);
        PartitionByCRC32PreSlot partition = new PartitionByCRC32PreSlot();
        ShardingTableHandler shardingTableHandler = TableHandlerMocks.mockTableHandlerWithDataNodes(count);
        partition.init(shardingTableHandler, Collections.singletonMap("count", text), Collections.emptyMap());
        return partition;
    }

}