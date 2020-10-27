package io.mycat.router.mycat1xfunction;

import io.mycat.router.ShardingTableHandler;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PartitionByDateTest {

    @Test
    public  void baseTest() {
        PartitionByDate partition = new PartitionByDate();

        Map<String, String> prot = new HashMap<>();
        prot.put("beginDate", "2014-01-01");
        prot.put("endDate", null);
        prot.put("partionDay", "10");
        prot.put("dateFormat", "yyyy-MM-dd");

        ShardingTableHandler shardingTableHandler = TableHandlerMocks.mockTableHandlerWithDataNodes(1024);
        partition.init(shardingTableHandler,prot, Collections.emptyMap());


        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-01"));
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-10"));
        Assert.assertEquals(true, 1 == partition.calculateIndex("2014-01-11"));
        Assert.assertEquals(true, 12 == partition.calculateIndex("2014-05-01"));


        //////////////////////////////////////////
        prot.clear();
        //////////////////////////////////////////

        prot.put("beginDate", "2014-01-01");
        prot.put("endDate", "2014-01-31");
        prot.put("partionDay", "10");
        prot.put("dateFormat", "yyyy-MM-dd");
        partition.init(shardingTableHandler,prot, Collections.emptyMap());

//
//		/**
//		 * 0 : 01.01-01.10,02.10-02.19
//		 * 1 : 01.11-01.20,02.20-03.01
//		 * 2 : 01.21-01.30,03.02-03.12
//		 * 3  ： 01.31-02-09,03.13-03.23
//		 */
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-01"));
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-10"));
        Assert.assertEquals(true, 1 == partition.calculateIndex("2014-01-11"));
        Assert.assertEquals(true, 3 == partition.calculateIndex("2014-02-01"));
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-02-19"));
        Assert.assertEquals(true, 1 == partition.calculateIndex("2014-02-20"));
        Assert.assertEquals(true, 1 == partition.calculateIndex("2014-03-01"));
        Assert.assertEquals(true, 2 == partition.calculateIndex("2014-03-02"));
        Assert.assertEquals(true, 2 == partition.calculateIndex("2014-03-11"));
        Assert.assertEquals(true, 3 == partition.calculateIndex("2014-03-20"));


        //////////////////////////////////////////
        prot.clear();
        //////////////////////////////////////////


        prot.put("beginDate", "2014-01-01");
        prot.put("endDate", "2014-01-31");
        prot.put("partionDay", "1");
        prot.put("dateFormat", "yyyy-MM-dd");
        partition.init(shardingTableHandler,prot, Collections.emptyMap());

        //测试默认1
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-01"));
        Assert.assertEquals(true, 9 == partition.calculateIndex("2014-01-10"));
        Assert.assertEquals(true, 10 == partition.calculateIndex("2014-01-11"));
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-02-01"));
        System.out.println(partition.calculate("2014-02-19"));
    }
}