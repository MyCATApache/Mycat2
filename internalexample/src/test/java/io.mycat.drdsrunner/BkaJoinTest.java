package io.mycat.drdsrunner;

import com.google.common.collect.ImmutableMap;
import io.mycat.*;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.calcite.MycatRel;
import io.mycat.calcite.rewriter.OptimizationContext;
import io.mycat.calcite.rewriter.SQLRBORewriter;
import io.mycat.calcite.spm.Plan;
import io.mycat.calcite.spm.PlanImpl;
import io.mycat.calcite.spm.SpecificSql;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.util.NameMap;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.concurrent.TimeUnit;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class BkaJoinTest extends DrdsTest {


    @BeforeClass
    public static void beforeClass(){
        DrdsTest.drdsRunner = null;
        DrdsTest.metadataManager = null;
    }


    public static DrdsSqlCompiler getDrds() {
        DrdsSqlCompiler sqlCompiler = DrdsTest.getDrds();
        DrdsConst config = sqlCompiler.getConfig();
        DrdsSqlCompiler.RBO_MERGE_JOIN = false;
        DrdsSqlCompiler.RBO_BKA_JOIN = true;
        DrdsSqlCompiler.BKA_JOIN_LEFT_ROW_COUNT_LIMIT = 8000000;
        DrdsSqlCompiler drdsSqlCompiler = new DrdsSqlCompiler(new DrdsConst() {
            @Override
            public NameMap<SchemaHandler> schemas() {
                return config.schemas();
            }
        });
        return drdsSqlCompiler;
    }

    public static Explain parse(String sql) {
        DrdsSqlCompiler drds = getDrds();
        DrdsSqlWithParams drdsSqlWithParams = DrdsRunnerHelper.preParse(sql, null);
        OptimizationContext optimizationContext = new OptimizationContext();
        MycatRel dispatch = drds.dispatch(optimizationContext, drdsSqlWithParams);
        Plan plan = new PlanImpl(dispatch, DrdsExecutorCompiler.getCodeExecuterContext(optimizationContext.relNodeContext.getConstantMap(), dispatch, false), drdsSqlWithParams.getAliasList());
        return new Explain(plan, drdsSqlWithParams);
    }

    @Test
    public void testSelectNormalNormal() throws Exception {
        Explain explain = parse("select * from db1.normal s join db1.normal2 e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=addressname}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=addressname0}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[[db1.normal, db1.normal2]])", explain.dumpPlan());
//        Assert.assertEquals("[SpecificSql(relNode=MycatView(distribution=[normalTables=db1.normal,db1.normal2]) , parameterizedSql=SELECT *  FROM db1.normal      INNER JOIN db1.normal2 ON (`normal`.`id` = `normal2`.`id`), sqls=[Each(targetName=prototype, sql=SELECT * FROM db1.normal     INNER JOIN db1.normal2 ON (`normal`.`id` = `normal2`.`id`))])]",
//                explain.specificSql().toString());
    }

    @Test
    public void testSelectNormalOtherNormal() throws Exception {
        Explain explain = parse("select * from db1.normal s join db1.normal3 e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=addressname}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=addressname0}]",
                explain.getColumnInfo());
        String dumpPlan = explain.dumpPlan();
        Assert.assertEquals("MycatSQLTableLookup(condition=[=($0, $2)], joinType=[inner], type=[BACK], correlationIds=[[$cor0]], leftKeys=[[0]])   MycatView(distribution=[[db1.normal]])   MycatView(distribution=[[db1.normal3]])",dumpPlan.trim());
        Assert.assertEquals("[Each(targetName=ds1, sql=SELECT * FROM db1.normal3 AS `normal3` WHERE ((`normal3`.`id`) IN ($cor0))), Each(targetName=prototype, sql=SELECT * FROM db1.normal AS `normal`)]",
                explain.specificSql().toString());
        System.out.println();
    }

    @Test
    public void testSelectNormalGlobal() throws Exception {
        Explain explain = parse("select * from db1.normal s join db1.global e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=addressname}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}]",
                explain.getColumnInfo());
        String dumpPlan = explain.dumpPlan();
        Assert.assertEquals("MycatView(distribution=[[db1.global, db1.normal]])",dumpPlan);
//        Assert.assertEquals("[SpecificSql(relNode=MycatView(distribution=[normalTables=db1.normal,globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.normal      INNER JOIN db1.global ON (`normal`.`id` = `global`.`id`), sqls=[Each(targetName=prototype, sql=SELECT * FROM db1.normal     INNER JOIN db1.global ON (`normal`.`id` = `global`.`id`))])]",
//                explain.specificSql().toString());
    }

    @Test
    public void testSelectNormalSharding() throws Exception {
        Explain explain = parse("select * from db1.normal s join db1.sharding e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=addressname}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}]",
                explain.getColumnInfo());
        String dumpPlan = explain.dumpPlan();
        Assert.assertEquals(
                "MycatSQLTableLookup(condition=[=($0, $2)], joinType=[inner], type=[BACK], correlationIds=[[$cor0]], leftKeys=[[0]])   MycatView(distribution=[[db1.normal]])   MycatView(distribution=[[db1.sharding]], conditions=[IN(ROW($0), ROW(CAST($cor0):BIGINT NOT NULL))])"
                .trim(),
                dumpPlan.trim());
        Assert.assertEquals(
                "[Each(targetName=c0, sql=SELECT * FROM db1_0.sharding_0 AS `sharding` WHERE ((`sharding`.`id`) IN ($cor0)) union all SELECT * FROM db1_0.sharding_1 AS `sharding` WHERE ((`sharding`.`id`) IN ($cor0)))\n" +
                        "Each(targetName=c1, sql=SELECT * FROM db1_1.sharding_0 AS `sharding` WHERE ((`sharding`.`id`) IN ($cor0)) union all SELECT * FROM db1_1.sharding_1 AS `sharding` WHERE ((`sharding`.`id`) IN ($cor0))), Each(targetName=prototype, sql=SELECT * FROM db1.normal AS `normal`)]",
                explain.specificSql().toString());
    }

    @Test
    public void testSelectSharding() throws Exception {
        Explain explain = parse("select * from db1.sharding");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[[db1.sharding]])", explain.dumpPlan());
//        Assert.assertEquals(
//                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding]) , parameterizedSql=SELECT *  FROM db1.sharding, sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.sharding_0     UNION ALL     SELECT *     FROM db1_0.sharding_1)), Each(targetName=c1, sql=(SELECT *     FROM db1_1.sharding_0     UNION ALL     SELECT *     FROM db1_1.sharding_1))])]",
//                explain.specificSql().toString());
    }


    @Test
    public void testSelectShardingSelf() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.sharding e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=user_id0}, {columnType=DATE, nullable=true, columnName=traveldate0}, {columnType=DECIMAL, nullable=true, columnName=fee0}, {columnType=BIGINT, nullable=true, columnName=days0}, {columnType=VARBINARY, nullable=true, columnName=blob0}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[[db1.sharding, db1.sharding]])", explain.dumpPlan());
//        Assert.assertEquals("[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding,db1.sharding]) , parameterizedSql=SELECT *  FROM db1.sharding      INNER JOIN db1.sharding AS `sharding0` ON (`sharding`.`id` = `sharding0`.`id`), sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.sharding_0         INNER JOIN db1_0.sharding_0 AS `sharding_00` ON (`sharding_0`.`id` = `sharding_00`.`id`)     UNION ALL     SELECT *     FROM db1_0.sharding_1         INNER JOIN db1_0.sharding_1 AS `sharding_10` ON (`sharding_1`.`id` = `sharding_10`.`id`))), Each(targetName=c1, sql=(SELECT *     FROM db1_1.sharding_0         INNER JOIN db1_1.sharding_0 AS `sharding_00` ON (`sharding_0`.`id` = `sharding_00`.`id`)     UNION ALL     SELECT *     FROM db1_1.sharding_1         INNER JOIN db1_1.sharding_1 AS `sharding_10` ON (`sharding_1`.`id` = `sharding_10`.`id`)))])]",
//                explain.specificSql().toString());
    }

    @Test
    public void testSelectShardingSharding() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.other_sharding e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=user_id0}, {columnType=DATE, nullable=true, columnName=traveldate0}, {columnType=DECIMAL, nullable=true, columnName=fee0}, {columnType=BIGINT, nullable=true, columnName=days0}, {columnType=VARBINARY, nullable=true, columnName=blob0}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatSQLTableLookup(condition=[=($0, $6)], joinType=[inner], type=[BACK], correlationIds=[[$cor0]], leftKeys=[[0]])   MycatView(distribution=[[db1.sharding]])   MycatView(distribution=[[db1.other_sharding]], conditions=[IN(ROW($0), ROW(CAST($cor0):BIGINT NOT NULL))])", explain.dumpPlan().trim());
        Assert.assertEquals("[Each(targetName=c0, sql=SELECT * FROM db1_0.other_sharding_0 AS `other_sharding` WHERE ((`other_sharding`.`id`) IN ($cor0)) union all SELECT * FROM db1_0.other_sharding_1 AS `other_sharding` WHERE ((`other_sharding`.`id`) IN ($cor0)))\n" +
                        "Each(targetName=c1, sql=SELECT * FROM db1_1.other_sharding_0 AS `other_sharding` WHERE ((`other_sharding`.`id`) IN ($cor0)) union all SELECT * FROM db1_1.other_sharding_1 AS `other_sharding` WHERE ((`other_sharding`.`id`) IN ($cor0))), Each(targetName=c0, sql=SELECT * FROM db1_0.sharding_0 AS `sharding` union all SELECT * FROM db1_0.sharding_1 AS `sharding`)\n" +
                        "Each(targetName=c1, sql=SELECT * FROM db1_1.sharding_0 AS `sharding` union all SELECT * FROM db1_1.sharding_1 AS `sharding`)]",
                explain.specificSql().toString());
    }

    @Test
    public void testSelectShardingNormal() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.normal e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=addressname}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatSQLTableLookup(condition=[=($0, $6)], joinType=[inner], type=[BACK], correlationIds=[[$cor0]], leftKeys=[[0]])   MycatView(distribution=[[db1.sharding]])   MycatView(distribution=[[db1.normal]])"
                .trim(),
                explain.dumpPlan().trim());
        Assert.assertEquals("[Each(targetName=prototype, sql=SELECT * FROM db1.normal AS `normal` WHERE ((`normal`.`id`) IN ($cor0))), Each(targetName=c0, sql=SELECT * FROM db1_0.sharding_0 AS `sharding` union all SELECT * FROM db1_0.sharding_1 AS `sharding`)\n" +
                        "Each(targetName=c1, sql=SELECT * FROM db1_1.sharding_0 AS `sharding` union all SELECT * FROM db1_1.sharding_1 AS `sharding`)]",
                explain.specificSql().toString());
    }
    @Test
    public void testSelectShardingNormalGlobal() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.normal e on s.id = e.id join db1.global g on e.id = g.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=addressname}, {columnType=BIGINT, nullable=false, columnName=id1}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}]",
                explain.getColumnInfo());
        String explainText = explain.dumpPlan().trim();
        Assert.assertEquals(true,
                explainText.contains("MycatSQLTableLookup(condition=[=($0, $8)], joinType=[inner], type=[BACK]")
               );
        Assert.assertEquals(true,       explain.specificSql().toString().contains("WHERE ((`normal`.`id`) IN ($cor0)))"));
    }

    @Test
    public void testSelectShardingNormalNormal2() throws Exception {
        Explain explain = parse("/*+ mycat:no_hash_join() */select * from db1.sharding s join db1.normal e on s.id = e.id join db1.normal2 g on e.id = g.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=addressname}, {columnType=BIGINT, nullable=false, columnName=id1}, {columnType=VARCHAR, nullable=true, columnName=addressname0}]",
                explain.getColumnInfo());
        String explainText = explain.dumpPlan().trim();
        Assert.assertEquals("MycatSQLTableLookup(condition=[=($6, $8)], joinType=[inner], type=[BACK], correlationIds=[[$cor3]], leftKeys=[[6]])   MycatSQLTableLookup(condition=[=($0, $6)], joinType=[inner], type=[BACK], correlationIds=[[$cor0]], leftKeys=[[0]])     MycatView(distribution=[[db1.sharding]])     MycatView(distribution=[[db1.normal]])   MycatView(distribution=[[db1.normal2]])",
                explainText
        );
        Assert.assertEquals("[Each(targetName=prototype, sql=SELECT * FROM db1.normal2 AS `normal2` WHERE ((`normal2`.`id`) IN ($cor3))), Each(targetName=prototype, sql=SELECT * FROM db1.normal AS `normal` WHERE ((`normal`.`id`) IN ($cor0))), Each(targetName=c0, sql=SELECT * FROM db1_0.sharding_0 AS `sharding` union all SELECT * FROM db1_0.sharding_1 AS `sharding`)\n" +
                        "Each(targetName=c1, sql=SELECT * FROM db1_1.sharding_0 AS `sharding` union all SELECT * FROM db1_1.sharding_1 AS `sharding`)]",
                explain.specificSql().toString());
    }

    @Test
    public void testSelectShardingNormalNormal3() throws Exception {
        Explain explain = parse("/*+ mycat:use_bka_join() */select * from db1.normal e join db1.normal2 g on e.id = g.id join db1.sharding s on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=addressname}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=addressname0}, {columnType=BIGINT, nullable=false, columnName=id1}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}]",
                explain.getColumnInfo());
        String explainText = explain.dumpPlan().trim();
        Assert.assertEquals("MycatSQLTableLookup(condition=[=($4, $0)], joinType=[inner], type=[BACK], correlationIds=[[$cor0]], leftKeys=[[0]])   MycatView(distribution=[[db1.normal, db1.normal2]])   MycatView(distribution=[[db1.sharding]], conditions=[IN(ROW($0), ROW(CAST($cor0):BIGINT NOT NULL))])",
                explainText
        );
        Assert.assertEquals("[Each(targetName=c0, sql=SELECT * FROM db1_0.sharding_0 AS `sharding` WHERE ((`sharding`.`id`) IN ($cor0)) union all SELECT * FROM db1_0.sharding_1 AS `sharding` WHERE ((`sharding`.`id`) IN ($cor0)))\n" +
                        "Each(targetName=c1, sql=SELECT * FROM db1_1.sharding_0 AS `sharding` WHERE ((`sharding`.`id`) IN ($cor0)) union all SELECT * FROM db1_1.sharding_1 AS `sharding` WHERE ((`sharding`.`id`) IN ($cor0))), Each(targetName=prototype, sql=SELECT * FROM db1.normal AS `normal`     INNER JOIN db1.normal2 AS `normal2` ON (`normal`.`id` = `normal2`.`id`))]",
                explain.specificSql().toString());
    }

    @Test
    public void testSelectShardingNormalNormal4() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.normal e on s.id = e.id join db1.normal2 g on e.id = g.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=addressname}, {columnType=BIGINT, nullable=false, columnName=id1}, {columnType=VARCHAR, nullable=true, columnName=addressname0}]",
                explain.getColumnInfo());
        String explainText = explain.dumpPlan().trim();
        Assert.assertEquals("MycatProject(id=[$4], user_id=[$5], traveldate=[$6], fee=[$7], days=[$8], blob=[$9], id0=[$0], addressname=[$1], id1=[$2], addressname0=[$3])   MycatHashJoin(condition=[=($0, $4)], joinType=[inner])     MycatView(distribution=[[db1.normal, db1.normal2]])     MycatView(distribution=[[db1.sharding]])",
                explainText
        );
        Assert.assertEquals("[Each(targetName=prototype, sql=SELECT * FROM db1.normal AS `normal`     INNER JOIN db1.normal2 AS `normal2` ON (`normal`.`id` = `normal2`.`id`)), Each(targetName=c0, sql=SELECT * FROM db1_0.sharding_0 AS `sharding` union all SELECT * FROM db1_0.sharding_1 AS `sharding`)\n" +
                        "Each(targetName=c1, sql=SELECT * FROM db1_1.sharding_0 AS `sharding` union all SELECT * FROM db1_1.sharding_1 AS `sharding`)]",
                explain.specificSql().toString());
    }



    @Test
    public void testSelectShardingGlobal() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.global e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[[db1.global, db1.sharding]])", explain.dumpPlan());
//        Assert.assertEquals(
//                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding,globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.sharding      INNER JOIN db1.global ON (`sharding`.`id` = `global`.`id`), sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.sharding_0         INNER JOIN db1.global ON (`sharding_0`.`id` = `global`.`id`)     UNION ALL     SELECT *     FROM db1_0.sharding_1         INNER JOIN db1.global AS `global0` ON (`sharding_1`.`id` = `global0`.`id`))), Each(targetName=c1, sql=(SELECT *     FROM db1_1.sharding_0         INNER JOIN db1.global ON (`sharding_0`.`id` = `global`.`id`)     UNION ALL     SELECT *     FROM db1_1.sharding_1         INNER JOIN db1.global AS `global0` ON (`sharding_1`.`id` = `global0`.`id`)))])]",
//                explain.specificSql().toString());
    }

    @Test
    public void testSelectShardingERWhere() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.er e on s.id = e.id where s.id = 1");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=user_id0}, {columnType=DATE, nullable=true, columnName=traveldate0}, {columnType=DECIMAL, nullable=true, columnName=fee0}, {columnType=BIGINT, nullable=true, columnName=days0}, {columnType=VARBINARY, nullable=true, columnName=blob0}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[[db1.er, db1.sharding]], conditions=[=($0, CAST(?0):BIGINT NOT NULL)])", explain.dumpPlan());
        System.out.println(explain.specificSql());
//        Assert.assertEquals(
//                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding], conditions=[=(CAST($0):DECIMAL(19, 0), ?0)]) , parameterizedSql=SELECT *  FROM db1.sharding  WHERE (CAST(`id` AS decimal) = ?), sqls=[Each(targetName=c0, sql=SELECT * FROM db1_0.sharding_1 WHERE (CAST(`id` AS decimal) = ?))]), SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.er], conditions=[=(CAST($0):DECIMAL(19, 0), ?0)]) , parameterizedSql=SELECT *  FROM db1.er  WHERE (CAST(`id` AS decimal) = ?), sqls=[Each(targetName=c0, sql=SELECT * FROM db1_0.er_1 WHERE (CAST(`id` AS decimal) = ?))])]",
//                explain.specificSql().toString());
    }

    @Test
    public void testSelectShardingER() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.er e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=user_id0}, {columnType=DATE, nullable=true, columnName=traveldate0}, {columnType=DECIMAL, nullable=true, columnName=fee0}, {columnType=BIGINT, nullable=true, columnName=days0}, {columnType=VARBINARY, nullable=true, columnName=blob0}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[[db1.er, db1.sharding]])", explain.dumpPlan());
        System.out.println(explain.specificSql());
//        Assert.assertEquals(
//                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding,db1.er]) , parameterizedSql=SELECT *  FROM db1.sharding      INNER JOIN db1.er ON (`sharding`.`id` = `er`.`id`), sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.sharding_0         INNER JOIN db1_0.er_0 ON (`sharding_0`.`id` = `er_0`.`id`)     UNION ALL     SELECT *     FROM db1_0.sharding_1         INNER JOIN db1_0.er_1 ON (`sharding_1`.`id` = `er_1`.`id`))), Each(targetName=c1, sql=(SELECT *     FROM db1_1.sharding_0         INNER JOIN db1_1.er_0 ON (`sharding_0`.`id` = `er_0`.`id`)     UNION ALL     SELECT *     FROM db1_1.sharding_1         INNER JOIN db1_1.er_1 ON (`sharding_1`.`id` = `er_1`.`id`)))])]",
//                explain.specificSql().toString());
    }


    @Test
    public void testSelectShardingERGlobal() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.er e on s.id = e.id join  db1.global g on  e.id = g.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=user_id0}, {columnType=DATE, nullable=true, columnName=traveldate0}, {columnType=DECIMAL, nullable=true, columnName=fee0}, {columnType=BIGINT, nullable=true, columnName=days0}, {columnType=VARBINARY, nullable=true, columnName=blob0}, {columnType=BIGINT, nullable=false, columnName=id1}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}]",
                explain.getColumnInfo());
        Assert.assertTrue(explain.dumpPlan().contains("MycatView(distribution=[[db1.er, db1.global, db1.sharding]])"));
        System.out.println(explain.specificSql());
//        Assert.assertEquals(
//                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding,db1.er,globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.sharding      INNER JOIN db1.er ON (`sharding`.`id` = `er`.`id`)      INNER JOIN db1.global ON (`er`.`id` = `global`.`id`), sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.sharding_0         INNER JOIN db1_0.er_0 ON (`sharding_0`.`id` = `er_0`.`id`)         INNER JOIN db1.global ON (`er_0`.`id` = `global`.`id`)     UNION ALL     SELECT *     FROM db1_0.sharding_1         INNER JOIN db1_0.er_1 ON (`sharding_1`.`id` = `er_1`.`id`)         INNER JOIN db1.global AS `global0` ON (`er_1`.`id` = `global0`.`id`))), Each(targetName=c1, sql=(SELECT *     FROM db1_1.sharding_0         INNER JOIN db1_1.er_0 ON (`sharding_0`.`id` = `er_0`.`id`)         INNER JOIN db1.global ON (`er_0`.`id` = `global`.`id`)     UNION ALL     SELECT *     FROM db1_1.sharding_1         INNER JOIN db1_1.er_1 ON (`sharding_1`.`id` = `er_1`.`id`)         INNER JOIN db1.global AS `global0` ON (`er_1`.`id` = `global0`.`id`)))])]",
//                explain.specificSql().toString());
    }

    @Test
    public void testSelectShardingGlobalER() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.global g  on s.id = g.id join  db1.er e  on  e.id = s.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}, {columnType=BIGINT, nullable=false, columnName=id1}, {columnType=VARCHAR, nullable=true, columnName=user_id0}, {columnType=DATE, nullable=true, columnName=traveldate0}, {columnType=DECIMAL, nullable=true, columnName=fee0}, {columnType=BIGINT, nullable=true, columnName=days0}, {columnType=VARBINARY, nullable=true, columnName=blob0}]",
                explain.getColumnInfo());
        Assert.assertTrue(explain.dumpPlan().contains("MycatView(distribution=[[db1.er, db1.global, db1.sharding]])"));
        System.out.println(explain.specificSql());
//        Assert.assertEquals(
//                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding,globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.sharding      INNER JOIN db1.global ON (`sharding`.`id` = `global`.`id`), sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.sharding_0         INNER JOIN db1.global ON (`sharding_0`.`id` = `global`.`id`)     UNION ALL     SELECT *     FROM db1_0.sharding_1         INNER JOIN db1.global AS `global0` ON (`sharding_1`.`id` = `global0`.`id`))), Each(targetName=c1, sql=(SELECT *     FROM db1_1.sharding_0         INNER JOIN db1.global ON (`sharding_0`.`id` = `global`.`id`)     UNION ALL     SELECT *     FROM db1_1.sharding_1         INNER JOIN db1.global AS `global0` ON (`sharding_1`.`id` = `global0`.`id`)))]), SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.er]) , parameterizedSql=SELECT *  FROM db1.er, sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.er_0     UNION ALL     SELECT *     FROM db1_0.er_1)), Each(targetName=c1, sql=(SELECT *     FROM db1_1.er_0     UNION ALL     SELECT *     FROM db1_1.er_1))])]",
//                explain.specificSql().toString());
    }

    @Test
    public void testSelectShardingGlobalER2() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.global g  on s.id = g.id join  db1.er e  on  e.id = g.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}, {columnType=BIGINT, nullable=false, columnName=id1}, {columnType=VARCHAR, nullable=true, columnName=user_id0}, {columnType=DATE, nullable=true, columnName=traveldate0}, {columnType=DECIMAL, nullable=true, columnName=fee0}, {columnType=BIGINT, nullable=true, columnName=days0}, {columnType=VARBINARY, nullable=true, columnName=blob0}]",
                explain.getColumnInfo());
        Assert.assertTrue(explain.dumpPlan().contains("MycatView(distribution=[[db1.er, db1.global, db1.sharding]])"));
        System.out.println(explain.specificSql());
    }

    @Test
    public void testSelectShardingGlobalBadColumn() throws Exception {
        Explain explain = parse("select t2.id from db1.sharding t2 join db1.normal t1 on t2.id = t1.id join db1.er l2 on t2.id = l2.id;");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}]",
                explain.getColumnInfo());
        System.out.println(explain.dumpPlan());
        System.out.println(explain.specificSql());
    }

    @Test
    public void testSelectGlobalShardingBadColumn() throws Exception {
        Explain explain = parse("select * from  db1.global g  join  db1.sharding s on s.id = g.id join  db1.er e  on  e.id = g.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=false, columnName=id1}, {columnType=VARCHAR, nullable=true, columnName=user_id0}, {columnType=DATE, nullable=true, columnName=traveldate0}, {columnType=DECIMAL, nullable=true, columnName=fee0}, {columnType=BIGINT, nullable=true, columnName=days0}, {columnType=VARBINARY, nullable=true, columnName=blob0}]",
                explain.getColumnInfo());
        Assert.assertTrue(explain.dumpPlan().contains("MycatView(distribution=[[db1.er, db1.global, db1.sharding]])"));
        System.out.println(explain.specificSql());
//        Assert.assertEquals(
//                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding,globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.global      INNER JOIN db1.sharding ON (`global`.`id` = `sharding`.`id`), sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1.global         INNER JOIN db1_0.sharding_0 ON (`global`.`id` = `sharding_0`.`id`)     UNION ALL     SELECT *     FROM db1.global AS `global0`         INNER JOIN db1_0.sharding_1 ON (`global0`.`id` = `sharding_1`.`id`))), Each(targetName=c1, sql=(SELECT *     FROM db1.global         INNER JOIN db1_1.sharding_0 ON (`global`.`id` = `sharding_0`.`id`)     UNION ALL     SELECT *     FROM db1.global AS `global0`         INNER JOIN db1_1.sharding_1 ON (`global0`.`id` = `sharding_1`.`id`)))]), SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.er]) , parameterizedSql=SELECT *  FROM db1.er, sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.er_0     UNION ALL     SELECT *     FROM db1_0.er_1)), Each(targetName=c1, sql=(SELECT *     FROM db1_1.er_0     UNION ALL     SELECT *     FROM db1_1.er_1))])]",
//                explain.specificSql().toString());
    }


    @Test
    public void testSelectGlobalNormal() throws Exception {
        Explain explain = parse("select * from db1.global s join db1.normal e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=addressname}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[[db1.global, db1.normal]])", explain.dumpPlan());
//        Assert.assertEquals(
//                "[SpecificSql(relNode=MycatView(distribution=[normalTables=db1.normal,globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.global      INNER JOIN db1.normal ON (`global`.`id` = `normal`.`id`), sqls=[Each(targetName=prototype, sql=SELECT * FROM db1.global     INNER JOIN db1.normal ON (`global`.`id` = `normal`.`id`))])]",
//                explain.specificSql().toString());

    }


}
