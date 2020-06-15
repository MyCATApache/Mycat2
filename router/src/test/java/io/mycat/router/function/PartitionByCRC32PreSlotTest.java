package io.mycat.router.function;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class PartitionByCRC32PreSlotTest {

    @Test
    public void test() {
        PartitionByCRC32PreSlot partition = getPartitionByCRC32PreSlot(1000);
        Assert.assertEquals(521, partition.calculate("1000316"));
        Assert.assertEquals(637, partition.calculate("2"));
    }

    @Test
    public void test2() {
        PartitionByCRC32PreSlot partition = getPartitionByCRC32PreSlot(2);


        Assert.assertEquals(0, partition.calculate("1"));
        Assert.assertEquals(1, partition.calculate("2"));
        Assert.assertEquals(0, partition.calculate("3"));
        Assert.assertEquals(1, partition.calculate("4"));
        Assert.assertEquals(0, partition.calculate("5"));
        Assert.assertEquals(0, partition.calculate("6"));
        Assert.assertEquals(0, partition.calculate("7"));
        Assert.assertEquals(0, partition.calculate("8"));
        Assert.assertEquals(0, partition.calculate("9"));

        Assert.assertEquals(0, partition.calculate("9999"));
        Assert.assertEquals(1, partition.calculate("123456789"));
        Assert.assertEquals(1, partition.calculate("35565"));

    }

    @Test
    public void test3() {
        PartitionByCRC32PreSlot partition = getPartitionByCRC32PreSlot(3);


        Assert.assertEquals(1 , partition.calculate("1"));
        Assert.assertEquals( 1 , partition.calculate("2"));
        Assert.assertEquals( 0 , partition.calculate("3"));
        Assert.assertEquals(2 , partition.calculate("4"));
        Assert.assertEquals(0 , partition.calculate("5"));
        Assert.assertEquals(1 , partition.calculate("6"));
        Assert.assertEquals(1 , partition.calculate("7"));
        Assert.assertEquals( 0 , partition.calculate("8"));
        Assert.assertEquals(0 , partition.calculate("9"));

        Assert.assertEquals(0 , partition.calculate("9999"));
        Assert.assertEquals(2 , partition.calculate("123456789"));
        Assert.assertEquals(2 , partition.calculate("35565"));

    }

    private PartitionByCRC32PreSlot getPartitionByCRC32PreSlot(int count) {
        String text = String.valueOf(count);
        PartitionByCRC32PreSlot partition = new PartitionByCRC32PreSlot();
        partition.init(null,Collections.singletonMap("count", text), Collections.emptyMap());
        return partition;
    }
}