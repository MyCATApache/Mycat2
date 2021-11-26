package io.mycat.drdsrunner;


import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class DistinctTest extends DrdsTest {

    @BeforeClass
    public static void beforeClass() {
        DrdsTest.drdsRunner = null;
        DrdsTest.metadataManager = null;
    }


    @Test
    public void testSelectDistinctStar() throws Exception {
        Explain explain = parse("SELECT DISTINCT *  from db1.distinct_sharding");

        Assert.assertEquals("[Each(targetName=c0, sql=SELECT `distinct_sharding`.`id`, `distinct_sharding`.`user_id`, `distinct_sharding`.`traveldate`, `distinct_sharding`.`fee`, `distinct_sharding`.`days`, `distinct_sharding`.`blob` FROM db1_0.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`id`, `distinct_sharding`.`user_id`, `distinct_sharding`.`traveldate`, `distinct_sharding`.`fee`, `distinct_sharding`.`days`, `distinct_sharding`.`blob` union all SELECT `distinct_sharding`.`id`, `distinct_sharding`.`user_id`, `distinct_sharding`.`traveldate`, `distinct_sharding`.`fee`, `distinct_sharding`.`days`, `distinct_sharding`.`blob` FROM db1_0.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`id`, `distinct_sharding`.`user_id`, `distinct_sharding`.`traveldate`, `distinct_sharding`.`fee`, `distinct_sharding`.`days`, `distinct_sharding`.`blob`)\n" +
                "Each(targetName=c1, sql=SELECT `distinct_sharding`.`id`, `distinct_sharding`.`user_id`, `distinct_sharding`.`traveldate`, `distinct_sharding`.`fee`, `distinct_sharding`.`days`, `distinct_sharding`.`blob` FROM db1_1.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`id`, `distinct_sharding`.`user_id`, `distinct_sharding`.`traveldate`, `distinct_sharding`.`fee`, `distinct_sharding`.`days`, `distinct_sharding`.`blob` union all SELECT `distinct_sharding`.`id`, `distinct_sharding`.`user_id`, `distinct_sharding`.`traveldate`, `distinct_sharding`.`fee`, `distinct_sharding`.`days`, `distinct_sharding`.`blob` FROM db1_1.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`id`, `distinct_sharding`.`user_id`, `distinct_sharding`.`traveldate`, `distinct_sharding`.`fee`, `distinct_sharding`.`days`, `distinct_sharding`.`blob`)]",explain.specificSql().toString());
        Assert.assertEquals("MycatView(distribution=[[db1.distinct_sharding]])",explain.dumpPlan().toString());
        System.out.println();
    }
    @Test
    public void testSelectDistinctShardingKeyButNoPrimaryKey() throws Exception {
        Explain explain = parse("SELECT DISTINCT id  from db1.distinct_sharding");

        Assert.assertEquals("[Each(targetName=c0, sql=SELECT `distinct_sharding`.`id` FROM db1_0.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`id` union all SELECT `distinct_sharding`.`id` FROM db1_0.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`id`)\n" +
                "Each(targetName=c1, sql=SELECT `distinct_sharding`.`id` FROM db1_1.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`id` union all SELECT `distinct_sharding`.`id` FROM db1_1.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`id`)]",explain.specificSql().toString());
        Assert.assertEquals("MycatView(distribution=[[db1.distinct_sharding]])",explain.dumpPlan().toString());
        System.out.println();
    }

    @Test
    public void testSelectDistinctNoShardingKeyButNoPrimaryKey() throws Exception {
        Explain explain = parse("SELECT DISTINCT traveldate  from db1.distinct_sharding");

        Assert.assertEquals("[Each(targetName=c0, sql=SELECT `distinct_sharding`.`traveldate` FROM db1_0.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`traveldate` union all SELECT `distinct_sharding`.`traveldate` FROM db1_0.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`traveldate`)\n" +
                "Each(targetName=c1, sql=SELECT `distinct_sharding`.`traveldate` FROM db1_1.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`traveldate` union all SELECT `distinct_sharding`.`traveldate` FROM db1_1.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`traveldate`)]",explain.specificSql().toString());
        Assert.assertEquals("MycatHashAggregate(group=[{0}])   MycatView(distribution=[[db1.distinct_sharding]])",explain.dumpPlan().toString());
        System.out.println();
    }
    @Test
    public void testSelectDistinctPrimaryKeyButNoShardingKey() throws Exception {
        Explain explain = parse("SELECT DISTINCT id  from db1.distinct_sharding");

        Assert.assertEquals("[Each(targetName=c0, sql=SELECT `distinct_sharding`.`id` FROM db1_0.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`id` union all SELECT `distinct_sharding`.`id` FROM db1_0.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`id`)\n" +
                "Each(targetName=c1, sql=SELECT `distinct_sharding`.`id` FROM db1_1.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`id` union all SELECT `distinct_sharding`.`id` FROM db1_1.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`id`)]",explain.specificSql().toString());
        Assert.assertEquals("MycatView(distribution=[[db1.distinct_sharding]])",explain.dumpPlan().toString());
        System.out.println();
    }

    @Test
    public void testSelectDistinctPrimaryKeyAndShardingKey() throws Exception {
        Explain explain = parse("SELECT DISTINCT id  from db1.sharding");

        Assert.assertEquals("[Each(targetName=c0, sql=SELECT `sharding`.`id` FROM db1_0.sharding_0 AS `sharding` union all SELECT `sharding`.`id` FROM db1_0.sharding_1 AS `sharding`)\n" +
                "Each(targetName=c1, sql=SELECT `sharding`.`id` FROM db1_1.sharding_0 AS `sharding` union all SELECT `sharding`.`id` FROM db1_1.sharding_1 AS `sharding`)]",explain.specificSql().toString());
        Assert.assertEquals("MycatView(distribution=[[db1.sharding]])",explain.dumpPlan().toString());
        System.out.println();
    }

    @Test
    public void testSelectDistinctPrimaryKeyAndShardingKey2() throws Exception {
        Explain explain = parse("SELECT DISTINCT user_id  from db1.distinct_sharding");

        Assert.assertEquals("[Each(targetName=c0, sql=SELECT `distinct_sharding`.`user_id` FROM db1_0.sharding_0 AS `distinct_sharding` union all SELECT `distinct_sharding`.`user_id` FROM db1_0.sharding_1 AS `distinct_sharding`)\n" +
                "Each(targetName=c1, sql=SELECT `distinct_sharding`.`user_id` FROM db1_1.sharding_0 AS `distinct_sharding` union all SELECT `distinct_sharding`.`user_id` FROM db1_1.sharding_1 AS `distinct_sharding`)]",explain.specificSql().toString());
        Assert.assertEquals("MycatView(distribution=[[db1.distinct_sharding]])",explain.dumpPlan().toString());
        System.out.println();
    }

    @Test
    public void testGroupByPrimaryKey() throws Exception {
        Explain explain = parse("SELECT user_id  from db1.distinct_sharding group by user_id");

        Assert.assertEquals("[Each(targetName=c0, sql=SELECT `distinct_sharding`.`user_id` FROM db1_0.sharding_0 AS `distinct_sharding` union all SELECT `distinct_sharding`.`user_id` FROM db1_0.sharding_1 AS `distinct_sharding`)\n" +
                "Each(targetName=c1, sql=SELECT `distinct_sharding`.`user_id` FROM db1_1.sharding_0 AS `distinct_sharding` union all SELECT `distinct_sharding`.`user_id` FROM db1_1.sharding_1 AS `distinct_sharding`)]",explain.specificSql().toString());
        Assert.assertEquals("MycatView(distribution=[[db1.distinct_sharding]])".trim(),explain.dumpPlan().toString());
        System.out.println();
    }
    @Test
    public void testGroupBy2Key() throws Exception {
        Explain explain = parse("SELECT user_id,id  from db1.distinct_sharding group by user_id,id");

        Assert.assertEquals("[Each(targetName=c0, sql=SELECT `distinct_sharding`.`user_id`, `distinct_sharding`.`id` FROM db1_0.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`user_id`, `distinct_sharding`.`id` union all SELECT `distinct_sharding`.`user_id`, `distinct_sharding`.`id` FROM db1_0.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`user_id`, `distinct_sharding`.`id`)\n" +
                "Each(targetName=c1, sql=SELECT `distinct_sharding`.`user_id`, `distinct_sharding`.`id` FROM db1_1.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`user_id`, `distinct_sharding`.`id` union all SELECT `distinct_sharding`.`user_id`, `distinct_sharding`.`id` FROM db1_1.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`user_id`, `distinct_sharding`.`id`)]",explain.specificSql().toString());
        Assert.assertEquals("MycatView(distribution=[[db1.distinct_sharding]])",explain.dumpPlan().toString());
        System.out.println();
    }
    @Test
    public void testCountNoShardingKey() throws Exception {
        Explain explain = parse("SELECT count(*)  from db1.distinct_sharding group by user_id");

        Assert.assertEquals("[Each(targetName=c0, sql=SELECT `distinct_sharding`.`user_id`, COUNT(*) AS `count(*)` FROM db1_0.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`user_id` union all SELECT `distinct_sharding`.`user_id`, COUNT(*) AS `count(*)` FROM db1_0.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`user_id`)\n" +
                "Each(targetName=c1, sql=SELECT `distinct_sharding`.`user_id`, COUNT(*) AS `count(*)` FROM db1_1.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`user_id` union all SELECT `distinct_sharding`.`user_id`, COUNT(*) AS `count(*)` FROM db1_1.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`user_id`)]",explain.specificSql().toString());

        Assert.assertEquals("MycatProject(count(*)=[$1])   MycatHashAggregate(group=[{0}], count(*)=[$SUM0($1)])     MycatView(distribution=[[db1.distinct_sharding]])",explain.dumpPlan().toString());
        System.out.println();
    }
    @Test
    public void testAvgNoShardingKey() throws Exception {
        Explain explain = parse("SELECT avg(fee)  from db1.distinct_sharding group by fee");

        Assert.assertEquals("[Each(targetName=c0, sql=SELECT `distinct_sharding`.`fee`, (COALESCE(SUM(`distinct_sharding`.`fee`), 0)) AS `$f1`, COUNT(`distinct_sharding`.`fee`) AS `$f2` FROM db1_0.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`fee` union all SELECT `distinct_sharding`.`fee`, (COALESCE(SUM(`distinct_sharding`.`fee`), 0)) AS `$f1`, COUNT(`distinct_sharding`.`fee`) AS `$f2` FROM db1_0.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`fee`)\n" +
                "Each(targetName=c1, sql=SELECT `distinct_sharding`.`fee`, (COALESCE(SUM(`distinct_sharding`.`fee`), 0)) AS `$f1`, COUNT(`distinct_sharding`.`fee`) AS `$f2` FROM db1_1.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`fee` union all SELECT `distinct_sharding`.`fee`, (COALESCE(SUM(`distinct_sharding`.`fee`), 0)) AS `$f1`, COUNT(`distinct_sharding`.`fee`) AS `$f2` FROM db1_1.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`fee`)]",explain.specificSql().toString());

        Assert.assertEquals("MycatProject(avg(fee)=[/(CAST(CASE(=($2, 0), null:DECIMAL(19, 0), $1)):DOUBLE, $2)])   MycatHashAggregate(group=[{0}], agg#0=[$SUM0($1)], agg#1=[$SUM0($2)])     MycatView(distribution=[[db1.distinct_sharding]])",explain.dumpPlan().toString());
        System.out.println();
    }
    @Test
    public void testMaxNoShardingKey() throws Exception {
        Explain explain = parse("SELECT max(fee)  from db1.distinct_sharding group by fee");

        Assert.assertEquals("[Each(targetName=c0, sql=SELECT `distinct_sharding`.`fee`, MAX(`distinct_sharding`.`fee`) AS `max(fee)` FROM db1_0.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`fee` union all SELECT `distinct_sharding`.`fee`, MAX(`distinct_sharding`.`fee`) AS `max(fee)` FROM db1_0.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`fee`)\n" +
                "Each(targetName=c1, sql=SELECT `distinct_sharding`.`fee`, MAX(`distinct_sharding`.`fee`) AS `max(fee)` FROM db1_1.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`fee` union all SELECT `distinct_sharding`.`fee`, MAX(`distinct_sharding`.`fee`) AS `max(fee)` FROM db1_1.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`fee`)]",explain.specificSql().toString());

        Assert.assertEquals("MycatProject(max(fee)=[$1])   MycatHashAggregate(group=[{0}], max(fee)=[MAX($1)])     MycatView(distribution=[[db1.distinct_sharding]])",explain.dumpPlan().toString());
        System.out.println();
    }
    @Test
    public void testCountShardingKey() throws Exception {
        Explain explain = parse("SELECT count(*)  from db1.distinct_sharding group by id");

        Assert.assertEquals("[Each(targetName=c0, sql=SELECT COUNT(*) AS `count(*)` FROM db1_0.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`id` union all SELECT COUNT(*) AS `count(*)` FROM db1_0.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`id`)\n" +
                "Each(targetName=c1, sql=SELECT COUNT(*) AS `count(*)` FROM db1_1.sharding_0 AS `distinct_sharding` GROUP BY `distinct_sharding`.`id` union all SELECT COUNT(*) AS `count(*)` FROM db1_1.sharding_1 AS `distinct_sharding` GROUP BY `distinct_sharding`.`id`)]",explain.specificSql().toString());

        Assert.assertEquals("MycatView(distribution=[[db1.distinct_sharding]])",explain.dumpPlan().toString());
        System.out.println();
    }
    @Test
    public void testDistinctUniqueKey() throws Exception {
        Explain explain = parse("SELECT days  from db1.distinct_sharding group by days");

        Assert.assertEquals("[Each(targetName=c0, sql=SELECT `distinct_sharding`.`days` FROM db1_0.sharding_0 AS `distinct_sharding` union all SELECT `distinct_sharding`.`days` FROM db1_0.sharding_1 AS `distinct_sharding`)\n" +
                "Each(targetName=c1, sql=SELECT `distinct_sharding`.`days` FROM db1_1.sharding_0 AS `distinct_sharding` union all SELECT `distinct_sharding`.`days` FROM db1_1.sharding_1 AS `distinct_sharding`)]",explain.specificSql().toString());

        Assert.assertEquals("MycatView(distribution=[[db1.distinct_sharding]])",explain.dumpPlan().toString());
        System.out.println();
    }


}
