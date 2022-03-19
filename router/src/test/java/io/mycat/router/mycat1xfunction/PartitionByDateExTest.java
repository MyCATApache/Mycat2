package io.mycat.router.mycat1xfunction;

import io.mycat.router.ShardingTableHandler;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PartitionByDateExTest {

    @Test
    public void test()  {
        PartitionByDateEx partition=new PartitionByDateEx();

        partition.setDateFormat("yyyy-MM-dd");
        partition.setsBeginDate("2014-01-01");
        partition.setsPartionDay("10");

        partition.init();

        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-01"));
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-10"));
        Assert.assertEquals(true, 1 == partition.calculateIndex("2014-01-11"));
        Assert.assertEquals(true, 12 == partition.calculateIndex("2014-05-01"));

        partition.setDateFormat("yyyy-MM-dd");
        partition.setsBeginDate("2014-01-01");
        partition.setsEndDate("2014-01-31");
        partition.setsPartionDay("10");
        partition.init();
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

        //测试默认1
        partition.setDateFormat("yyyy-MM-dd");
        partition.setsBeginDate("2014-01-01");
        partition.setsEndDate("2014-01-31");
        partition.setsPartionDay("1");
        partition.init();
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-01"));
        Assert.assertEquals(true, 9 == partition.calculateIndex("2014-01-10"));
        Assert.assertEquals(true, 10 == partition.calculateIndex("2014-01-11"));
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-02-01"));
        System.out.println(partition.calculateIndex("2014-02-19"));

        //自然日测试
        //1、只开启自然日分表开关
        PartitionByDateEx partition2=new PartitionByDateEx();
        partition2.setDateFormat("yyyy-MM-dd");
        partition2.setsNaturalDay("1");
        partition2.init();
        //Assert.assertEquals(true, 6 == partition2.calculate("2014-01-20"));
        Assert.assertEquals(true, 19 == partition2.calculateIndex("2014-01-20"));
        Assert.assertEquals(true, 0 == partition2.calculateIndex("2014-03-01"));
        Assert.assertEquals(true, 30 == partition2.calculateIndex("2018-03-31"));


        //2、顺便开启开始时间
        partition2.setDateFormat("yyyy-MM-dd");
        partition2.setsNaturalDay("1");
        partition2.setsPartionDay("1");
        partition2.setsBeginDate("2014-01-02");
        partition2.init();

        Assert.assertEquals(true, 19 == partition2.calculateIndex("2014-01-20"));
        Assert.assertEquals(true, 0 == partition2.calculateIndex("2014-03-01"));
        Assert.assertEquals(true, 30 == partition2.calculateIndex("2018-03-31"));

        //2、顺便开启开始时间,结束时间不足28天（开启自然日失败，默认间隔模式）PartionDay=1
        PartitionByDateEx partition3=new PartitionByDateEx();
        partition3.setDateFormat("yyyy-MM-dd");
        partition3.setsNaturalDay("1");
        partition3.setsPartionDay("1");
        partition3.setsBeginDate("2014-01-02");
        partition3.setsEndDate("2014-01-20");
        partition3.init();
        Assert.assertEquals(true, 0 == partition3.calculateIndex("2014-01-02"));
        Assert.assertEquals(true, 1 == partition3.calculateIndex("2014-01-03"));
        Assert.assertEquals(true, 2 == partition3.calculateIndex("2014-01-04"));
        Assert.assertEquals(true, 6 == partition3.calculateIndex("2014-01-08"));
        Assert.assertEquals(true, 8 == partition3.calculateIndex("2014-01-10"));
        Assert.assertEquals(true, 12 == partition3.calculateIndex("2014-01-14"));
        Assert.assertEquals(true, 18 == partition3.calculateIndex("2014-01-20"));
        System.out.println(partition3.calculateIndex("2014-03-01"));
        //Assert.assertEquals(true, 0 == partition3.calculate("2014-03-01"));

        //3、顺便开启开始时间,结束时间不足28天（开启自然日失败，默认间隔模式）PartionDay=10 恢复间隔模式
        partition.setsNaturalDay("1");
        partition.setsBeginDate("2014-01-01");
        partition.setsEndDate("2014-01-24");
        partition.setsPartionDay("10");
        partition.init();
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-01"));
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-10"));
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-05"));
        Assert.assertEquals(true, 1 == partition.calculateIndex("2014-01-20"));
        System.out.println("------------success!----");


        //4、顺便开启开始时间,结束时间超过29天 PartionDay=1
        partition.setsNaturalDay("1");
        partition.setsBeginDate("2014-01-01");
        partition.setsEndDate("2014-01-29");
        partition.setsPartionDay("10");
        partition.init();
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-01"));
        Assert.assertEquals(true, 9 == partition.calculateIndex("2014-01-10"));
        Assert.assertEquals(true, 4 == partition.calculateIndex("2014-01-05"));
        Assert.assertEquals(true, 19 == partition.calculateIndex("2014-01-20"));
        Assert.assertEquals(true, 30 == partition.calculateIndex("2018-01-31"));


        //4、顺便开启开始时间,结束时间超过29天 PartionDay=1
        partition.setsNaturalDay("1");
        partition.setsBeginDate("2014-01-01");
        partition.setsEndDate("2018-01-29");
        partition.setsPartionDay("1");
        partition.init();
        Assert.assertEquals(true, 0 == partition.calculateIndex("2014-01-01"));
        Assert.assertEquals(true, 9 == partition.calculateIndex("2014-01-10"));
        Assert.assertEquals(true, 4 == partition.calculateIndex("2014-01-05"));
        Assert.assertEquals(true, 19 == partition.calculateIndex("2014-01-20"));
        Assert.assertEquals(true, 30 == partition.calculateIndex("2018-01-31"));
        System.out.println("------------success!----");


        //4、顺便开启开始时间,结束时间超过29天 PartionDay=1
        partition.setsNaturalDay("0");
        partition.setsBeginDate("2021-01-01");
        partition.setsPartionDay("10");
        partition.init();
        Assert.assertEquals(0,partition.calculateIndex("2021-01-01"));
        Assert.assertEquals(365, partition.calculateIndex("2022-01-01"));
        System.out.println("------------success!----");
    }
}