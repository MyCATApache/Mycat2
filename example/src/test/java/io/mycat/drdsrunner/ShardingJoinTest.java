package io.mycat.drdsrunner;

import io.mycat.calcite.spm.SpecificSql;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ShardingJoinTest extends DrdsTest {
    @Test
    public void testSelect1() throws Exception {
        Explain explain = parse("select 1");
        Assert.assertEquals("[{columnType=INTEGER, nullable=false, columnName=1}]", explain.getColumnInfo());
        Assert.assertEquals("MycatProject(1=[1])   MycatValues(tuples=[[{ 0 }]]) ", explain.dumpPlan());
    }

    @Test
    public void testSelectNormal() throws Exception {
        Explain explain = parse("select * from db1.normal");
        Assert.assertEquals("[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=addressname}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[normalTables=db1.normal]) ", explain.dumpPlan());
        Assert.assertEquals(
                "[SpecificSql(relNode=MycatView(distribution=[normalTables=db1.normal]) , parameterizedSql=SELECT *  FROM db1.normal, sqls=[Each(targetName=prototype, sql=SELECT * FROM db1.normal)])]",
                explain.specificSql().toString());
    }
    @Test
    public void testSelectNormalNormal() throws Exception {
        Explain explain = parse("select * from db1.normal s join db1.normal2 e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=addressname}, {columnType=BIGINT, nullable=true, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=addressname0}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[normalTables=db1.normal,db1.normal2]) ", explain.dumpPlan());
        Assert.assertEquals("[SpecificSql(relNode=MycatView(distribution=[normalTables=db1.normal,db1.normal2]) , parameterizedSql=SELECT *  FROM db1.normal      INNER JOIN db1.normal2 ON (`normal`.`id` = `normal2`.`id`), sqls=[Each(targetName=prototype, sql=SELECT * FROM db1.normal     INNER JOIN db1.normal2 ON (`normal`.`id` = `normal2`.`id`))])]",
                explain.specificSql().toString());
    }


    @Test
    public void testSelectNormalGlobal() throws Exception {
        Explain explain = parse("select * from db1.normal s join db1.global e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=addressname}, {columnType=BIGINT, nullable=true, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[normalTables=db1.normal,globalTables=db1.global]) ", explain.dumpPlan());
        Assert.assertEquals("[SpecificSql(relNode=MycatView(distribution=[normalTables=db1.normal,globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.normal      INNER JOIN db1.global ON (`normal`.`id` = `global`.`id`), sqls=[Each(targetName=prototype, sql=SELECT * FROM db1.normal     INNER JOIN db1.global ON (`normal`.`id` = `global`.`id`))])]",
                explain.specificSql().toString());
    }
    @Test
    public void testSelectNormalSharding() throws Exception {
        Explain explain = parse("select * from db1.normal s join db1.sharding e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=addressname}, {columnType=BIGINT, nullable=true, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatHashJoin(condition=[=($0, $2)], joinType=[inner])   MycatView(distribution=[normalTables=db1.normal])   MycatView(distribution=[shardingTables=db1.sharding]) ", explain.dumpPlan());
        Assert.assertEquals(
                "[SpecificSql(relNode=MycatView(distribution=[normalTables=db1.normal]) , parameterizedSql=SELECT *  FROM db1.normal, sqls=[Each(targetName=prototype, sql=SELECT * FROM db1.normal)]), SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding]) , parameterizedSql=SELECT *  FROM db1.sharding, sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.sharding_0     UNION ALL     SELECT *     FROM db1_0.sharding_1)), Each(targetName=c1, sql=(SELECT *     FROM db1_1.sharding_0     UNION ALL     SELECT *     FROM db1_1.sharding_1))])]",
                explain.specificSql().toString());
    }
    @Test
    public void testSelectSharding() throws Exception {
        Explain explain = parse("select * from db1.sharding");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[shardingTables=db1.sharding]) ", explain.dumpPlan());
        Assert.assertEquals(
                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding]) , parameterizedSql=SELECT *  FROM db1.sharding, sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.sharding_0     UNION ALL     SELECT *     FROM db1_0.sharding_1)), Each(targetName=c1, sql=(SELECT *     FROM db1_1.sharding_0     UNION ALL     SELECT *     FROM db1_1.sharding_1))])]",
                explain.specificSql().toString());
    }

    @Test
    public void testSelectShardingWhere() throws Exception {
        Explain explain = parse("select * from db1.sharding where id = 1");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[shardingTables=db1.sharding], conditions=[=(CAST($0):DECIMAL(19, 0), ?0)]) ", explain.dumpPlan());
        Assert.assertEquals(
                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding], conditions=[=(CAST($0):DECIMAL(19, 0), ?0)]) , parameterizedSql=SELECT *  FROM db1.sharding  WHERE (CAST(`id` AS decimal) = ?), sqls=[Each(targetName=c0, sql=SELECT * FROM db1_0.sharding_1 WHERE (CAST(`id` AS decimal) = ?))])]",
                explain.specificSql().toString());
    }

    @Test
    public void testSelectShardingSelf() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.sharding e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=true, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=user_id0}, {columnType=DATE, nullable=true, columnName=traveldate0}, {columnType=DECIMAL, nullable=true, columnName=fee0}, {columnType=BIGINT, nullable=true, columnName=days0}, {columnType=VARBINARY, nullable=true, columnName=blob0}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[shardingTables=db1.sharding,db1.sharding]) ", explain.dumpPlan());
        Assert.assertEquals("[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding,db1.sharding]) , parameterizedSql=SELECT *  FROM db1.sharding      INNER JOIN db1.sharding AS `sharding0` ON (`sharding`.`id` = `sharding0`.`id`), sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.sharding_0         INNER JOIN db1_0.sharding_0 AS `sharding_00` ON (`sharding_0`.`id` = `sharding_00`.`id`)     UNION ALL     SELECT *     FROM db1_0.sharding_1         INNER JOIN db1_0.sharding_1 AS `sharding_10` ON (`sharding_1`.`id` = `sharding_10`.`id`))), Each(targetName=c1, sql=(SELECT *     FROM db1_1.sharding_0         INNER JOIN db1_1.sharding_0 AS `sharding_00` ON (`sharding_0`.`id` = `sharding_00`.`id`)     UNION ALL     SELECT *     FROM db1_1.sharding_1         INNER JOIN db1_1.sharding_1 AS `sharding_10` ON (`sharding_1`.`id` = `sharding_10`.`id`)))])]",
                explain.specificSql().toString());
    }
    @Test
    public void testSelectShardingSharding() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.other_sharding e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=true, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=user_id0}, {columnType=DATE, nullable=true, columnName=traveldate0}, {columnType=DECIMAL, nullable=true, columnName=fee0}, {columnType=BIGINT, nullable=true, columnName=days0}, {columnType=VARBINARY, nullable=true, columnName=blob0}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatHashJoin(condition=[=($0, $6)], joinType=[inner])   MycatView(distribution=[shardingTables=db1.sharding])   MycatView(distribution=[shardingTables=db1.other_sharding]) ", explain.dumpPlan());
        Assert.assertEquals("[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding]) , parameterizedSql=SELECT *  FROM db1.sharding, sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.sharding_0     UNION ALL     SELECT *     FROM db1_0.sharding_1)), Each(targetName=c1, sql=(SELECT *     FROM db1_1.sharding_0     UNION ALL     SELECT *     FROM db1_1.sharding_1))]), SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.other_sharding]) , parameterizedSql=SELECT *  FROM db1.other_sharding, sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.other_sharding_0     UNION ALL     SELECT *     FROM db1_0.other_sharding_1)), Each(targetName=c1, sql=(SELECT *     FROM db1_1.other_sharding_0     UNION ALL     SELECT *     FROM db1_1.other_sharding_1))])]",
                explain.specificSql().toString());
    }
    @Test
    public void testSelectShardingNormal() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.normal e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=true, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=addressname}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatHashJoin(condition=[=($0, $6)], joinType=[inner])   MycatView(distribution=[shardingTables=db1.sharding])   MycatView(distribution=[normalTables=db1.normal]) ", explain.dumpPlan());
        Assert.assertEquals("[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding]) , parameterizedSql=SELECT *  FROM db1.sharding, sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.sharding_0     UNION ALL     SELECT *     FROM db1_0.sharding_1)), Each(targetName=c1, sql=(SELECT *     FROM db1_1.sharding_0     UNION ALL     SELECT *     FROM db1_1.sharding_1))]), SpecificSql(relNode=MycatView(distribution=[normalTables=db1.normal]) , parameterizedSql=SELECT *  FROM db1.normal, sqls=[Each(targetName=prototype, sql=SELECT * FROM db1.normal)])]",
                explain.specificSql().toString());
    }

    @Test
    public void testSelectShardingGlobal() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.global e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=true, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[shardingTables=db1.sharding,globalTables=db1.global]) ", explain.dumpPlan());
        Assert.assertEquals(
                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding,globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.sharding      INNER JOIN db1.global ON (`sharding`.`id` = `global`.`id`), sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.sharding_0         INNER JOIN db1.global ON (`sharding_0`.`id` = `global`.`id`)     UNION ALL     SELECT *     FROM db1_0.sharding_1         INNER JOIN db1.global AS `global0` ON (`sharding_1`.`id` = `global0`.`id`))), Each(targetName=c1, sql=(SELECT *     FROM db1_1.sharding_0         INNER JOIN db1.global ON (`sharding_0`.`id` = `global`.`id`)     UNION ALL     SELECT *     FROM db1_1.sharding_1         INNER JOIN db1.global AS `global0` ON (`sharding_1`.`id` = `global0`.`id`)))])]",
                explain.specificSql().toString());
    }

    @Test
    public void testSelectShardingERWhere() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.er e on s.id = e.id where s.id = 1");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=true, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=user_id0}, {columnType=DATE, nullable=true, columnName=traveldate0}, {columnType=DECIMAL, nullable=true, columnName=fee0}, {columnType=BIGINT, nullable=true, columnName=days0}, {columnType=VARBINARY, nullable=true, columnName=blob0}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatHashJoin(condition=[=($0, $6)], joinType=[inner])   MycatView(distribution=[shardingTables=db1.sharding], conditions=[=(CAST($0):DECIMAL(19, 0), ?0)])   MycatView(distribution=[shardingTables=db1.er], conditions=[=(CAST($0):DECIMAL(19, 0), ?0)]) ", explain.dumpPlan());
        Assert.assertEquals(
                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding], conditions=[=(CAST($0):DECIMAL(19, 0), ?0)]) , parameterizedSql=SELECT *  FROM db1.sharding  WHERE (CAST(`id` AS decimal) = ?), sqls=[Each(targetName=c0, sql=SELECT * FROM db1_0.sharding_1 WHERE (CAST(`id` AS decimal) = ?))]), SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.er], conditions=[=(CAST($0):DECIMAL(19, 0), ?0)]) , parameterizedSql=SELECT *  FROM db1.er  WHERE (CAST(`id` AS decimal) = ?), sqls=[Each(targetName=c0, sql=SELECT * FROM db1_0.er_1 WHERE (CAST(`id` AS decimal) = ?))])]",
                explain.specificSql().toString());
    }
    @Test
    public void testSelectShardingER() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.er e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=true, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=user_id0}, {columnType=DATE, nullable=true, columnName=traveldate0}, {columnType=DECIMAL, nullable=true, columnName=fee0}, {columnType=BIGINT, nullable=true, columnName=days0}, {columnType=VARBINARY, nullable=true, columnName=blob0}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[shardingTables=db1.sharding,db1.er]) ", explain.dumpPlan());
        Assert.assertEquals(
                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding,db1.er]) , parameterizedSql=SELECT *  FROM db1.sharding      INNER JOIN db1.er ON (`sharding`.`id` = `er`.`id`), sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.sharding_0         INNER JOIN db1_0.er_0 ON (`sharding_0`.`id` = `er_0`.`id`)     UNION ALL     SELECT *     FROM db1_0.sharding_1         INNER JOIN db1_0.er_1 ON (`sharding_1`.`id` = `er_1`.`id`))), Each(targetName=c1, sql=(SELECT *     FROM db1_1.sharding_0         INNER JOIN db1_1.er_0 ON (`sharding_0`.`id` = `er_0`.`id`)     UNION ALL     SELECT *     FROM db1_1.sharding_1         INNER JOIN db1_1.er_1 ON (`sharding_1`.`id` = `er_1`.`id`)))])]",
                explain.specificSql().toString());
    }


    @Test
    public void testSelectShardingERGlobal() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.er e on s.id = e.id join  db1.global g on  e.id = g.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=true, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=user_id0}, {columnType=DATE, nullable=true, columnName=traveldate0}, {columnType=DECIMAL, nullable=true, columnName=fee0}, {columnType=BIGINT, nullable=true, columnName=days0}, {columnType=VARBINARY, nullable=true, columnName=blob0}, {columnType=BIGINT, nullable=true, columnName=id1}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[shardingTables=db1.sharding,db1.er,globalTables=db1.global]) ", explain.dumpPlan());
        Assert.assertEquals(
                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding,db1.er,globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.sharding      INNER JOIN db1.er ON (`sharding`.`id` = `er`.`id`)      INNER JOIN db1.global ON (`er`.`id` = `global`.`id`), sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.sharding_0         INNER JOIN db1_0.er_0 ON (`sharding_0`.`id` = `er_0`.`id`)         INNER JOIN db1.global ON (`er_0`.`id` = `global`.`id`)     UNION ALL     SELECT *     FROM db1_0.sharding_1         INNER JOIN db1_0.er_1 ON (`sharding_1`.`id` = `er_1`.`id`)         INNER JOIN db1.global AS `global0` ON (`er_1`.`id` = `global0`.`id`))), Each(targetName=c1, sql=(SELECT *     FROM db1_1.sharding_0         INNER JOIN db1_1.er_0 ON (`sharding_0`.`id` = `er_0`.`id`)         INNER JOIN db1.global ON (`er_0`.`id` = `global`.`id`)     UNION ALL     SELECT *     FROM db1_1.sharding_1         INNER JOIN db1_1.er_1 ON (`sharding_1`.`id` = `er_1`.`id`)         INNER JOIN db1.global AS `global0` ON (`er_1`.`id` = `global0`.`id`)))])]",
                explain.specificSql().toString());
    }
    @Test
    public void testSelectShardingGlobalER() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.global g  on s.id = g.id join  db1.er e  on  e.id = g.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=true, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}, {columnType=BIGINT, nullable=true, columnName=id1}, {columnType=VARCHAR, nullable=true, columnName=user_id0}, {columnType=DATE, nullable=true, columnName=traveldate0}, {columnType=DECIMAL, nullable=true, columnName=fee0}, {columnType=BIGINT, nullable=true, columnName=days0}, {columnType=VARBINARY, nullable=true, columnName=blob0}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatHashJoin(condition=[=($6, $9)], joinType=[inner])   MycatView(distribution=[shardingTables=db1.sharding,globalTables=db1.global])   MycatView(distribution=[shardingTables=db1.er]) ", explain.dumpPlan());
        Assert.assertEquals(
                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding,globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.sharding      INNER JOIN db1.global ON (`sharding`.`id` = `global`.`id`), sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.sharding_0         INNER JOIN db1.global ON (`sharding_0`.`id` = `global`.`id`)     UNION ALL     SELECT *     FROM db1_0.sharding_1         INNER JOIN db1.global AS `global0` ON (`sharding_1`.`id` = `global0`.`id`))), Each(targetName=c1, sql=(SELECT *     FROM db1_1.sharding_0         INNER JOIN db1.global ON (`sharding_0`.`id` = `global`.`id`)     UNION ALL     SELECT *     FROM db1_1.sharding_1         INNER JOIN db1.global AS `global0` ON (`sharding_1`.`id` = `global0`.`id`)))]), SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.er]) , parameterizedSql=SELECT *  FROM db1.er, sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.er_0     UNION ALL     SELECT *     FROM db1_0.er_1)), Each(targetName=c1, sql=(SELECT *     FROM db1_1.er_0     UNION ALL     SELECT *     FROM db1_1.er_1))])]",
                explain.specificSql().toString());
    }
    @Test
    public void testSelectGlobalShardingER() throws Exception {
        Explain explain = parse("select * from  db1.global g  join  db1.sharding s on s.id = g.id join  db1.er e  on  e.id = g.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}, {columnType=BIGINT, nullable=true, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=true, columnName=id1}, {columnType=VARCHAR, nullable=true, columnName=user_id0}, {columnType=DATE, nullable=true, columnName=traveldate0}, {columnType=DECIMAL, nullable=true, columnName=fee0}, {columnType=BIGINT, nullable=true, columnName=days0}, {columnType=VARBINARY, nullable=true, columnName=blob0}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatHashJoin(condition=[=($0, $9)], joinType=[inner])   MycatView(distribution=[shardingTables=db1.sharding,globalTables=db1.global])   MycatView(distribution=[shardingTables=db1.er]) ", explain.dumpPlan());
        Assert.assertEquals(
                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding,globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.global      INNER JOIN db1.sharding ON (`global`.`id` = `sharding`.`id`), sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1.global         INNER JOIN db1_0.sharding_0 ON (`global`.`id` = `sharding_0`.`id`)     UNION ALL     SELECT *     FROM db1.global AS `global0`         INNER JOIN db1_0.sharding_1 ON (`global0`.`id` = `sharding_1`.`id`))), Each(targetName=c1, sql=(SELECT *     FROM db1.global         INNER JOIN db1_1.sharding_0 ON (`global`.`id` = `sharding_0`.`id`)     UNION ALL     SELECT *     FROM db1.global AS `global0`         INNER JOIN db1_1.sharding_1 ON (`global0`.`id` = `sharding_1`.`id`)))]), SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.er]) , parameterizedSql=SELECT *  FROM db1.er, sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.er_0     UNION ALL     SELECT *     FROM db1_0.er_1)), Each(targetName=c1, sql=(SELECT *     FROM db1_1.er_0     UNION ALL     SELECT *     FROM db1_1.er_1))])]",
                explain.specificSql().toString());
    }
    @Test
    public void testSelectGlobal() throws Exception {
        Set<String> explainColumnSet = new HashSet<>();
        Set<String> explainSet = new HashSet<>();
        Set<List<SpecificSql>> sqlSet = new HashSet<>();
        long end = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
        Explain explain;
        while (true) {
             explain = parse("select * from db1.global");
            explainColumnSet.add(explain.getColumnInfo());
            explainSet.add(explain.dumpPlan());
            sqlSet.add(explain.specificSql());
            if (sqlSet.size() > 1 || (System.currentTimeMillis() > end)) {
                break;
            }
        }
        Assert.assertTrue(
                explainColumnSet.contains(
                        "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}]")
        );

        Assert.assertEquals("MycatView(distribution=[globalTables=db1.global]) ",explain.dumpPlan());
        Assert.assertEquals("[[SpecificSql(relNode=MycatView(distribution=[globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.global, sqls=[Each(targetName=c0, sql=SELECT * FROM db1.global)])], [SpecificSql(relNode=MycatView(distribution=[globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.global, sqls=[Each(targetName=c1, sql=SELECT * FROM db1.global)])]]",
                sqlSet.toString());

    }

    @Test
    public void testSelectGlobalNormal() throws Exception {
        Explain explain = parse("select * from db1.global s join db1.normal e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}, {columnType=BIGINT, nullable=true, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=addressname}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[normalTables=db1.normal,globalTables=db1.global]) ", explain.dumpPlan());
        Assert.assertEquals(
                "[SpecificSql(relNode=MycatView(distribution=[normalTables=db1.normal,globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.global      INNER JOIN db1.normal ON (`global`.`id` = `normal`.`id`), sqls=[Each(targetName=prototype, sql=SELECT * FROM db1.global     INNER JOIN db1.normal ON (`global`.`id` = `normal`.`id`))])]",
                explain.specificSql().toString());

    }

    @Test
    public void testSelectGlobalSharding() throws Exception {
        Explain explain = parse("select * from db1.global s join db1.sharding e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=true, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}, {columnType=BIGINT, nullable=true, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[shardingTables=db1.sharding,globalTables=db1.global]) ", explain.dumpPlan());
        Assert.assertEquals(
                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding,globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.global      INNER JOIN db1.sharding ON (`global`.`id` = `sharding`.`id`), sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1.global         INNER JOIN db1_0.sharding_0 ON (`global`.`id` = `sharding_0`.`id`)     UNION ALL     SELECT *     FROM db1.global AS `global0`         INNER JOIN db1_0.sharding_1 ON (`global0`.`id` = `sharding_1`.`id`))), Each(targetName=c1, sql=(SELECT *     FROM db1.global         INNER JOIN db1_1.sharding_0 ON (`global`.`id` = `sharding_0`.`id`)     UNION ALL     SELECT *     FROM db1.global AS `global0`         INNER JOIN db1_1.sharding_1 ON (`global0`.`id` = `sharding_1`.`id`)))])]",
                explain.specificSql().toString());

    }


}
