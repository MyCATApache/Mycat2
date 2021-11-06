package io.mycat.drdsrunner;


import io.mycat.*;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.calcite.MycatRel;
import io.mycat.calcite.rewriter.OptimizationContext;
import io.mycat.calcite.spm.Plan;
import io.mycat.calcite.spm.PlanImpl;
import io.mycat.calcite.spm.SpecificSql;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.config.*;
import io.mycat.prototypeserver.mysql.PrototypeService;
import io.mycat.util.JsonUtil;
import io.mycat.util.NameMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.concurrent.TimeUnit;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class PartitionKeyJoinTest extends DrdsTest {

    @BeforeClass
    public static void beforeClass() {
        DrdsTest.drdsRunner = null;
        DrdsTest.metadataManager = null;
    }


    public static DrdsSqlCompiler getDrds(DrdsConst drdsConst) {
        return new DrdsSqlCompiler(drdsConst);
    }

    public static Explain parse(DrdsConst drdsConst,String sql) {
        DrdsSqlCompiler drds = getDrds(drdsConst);
        DrdsSqlWithParams drdsSqlWithParams = DrdsRunnerHelper.preParse(sql, null);
        OptimizationContext optimizationContext = new OptimizationContext();
        MycatRel dispatch = drds.dispatch(optimizationContext, drdsSqlWithParams);
        Plan plan = new PlanImpl(dispatch, DrdsExecutorCompiler.getCodeExecuterContext(optimizationContext.relNodeContext.getConstantMap(), dispatch, false), drdsSqlWithParams.getAliasList());
        return new Explain(plan, drdsSqlWithParams);
    }


    @Test
    public void testSelectShardingInnerNormalInOneDb() throws Exception {
        Explain explain = parse(new DrdsConst() {
            @Override
            public NameMap<SchemaHandler> schemas() {
                Map<String, LogicSchemaConfig> schemaConfigs=new HashMap<>();
                LogicSchemaConfig logicSchemaConfig = new LogicSchemaConfig();
                Map<String, NormalTableConfig> normalTables = logicSchemaConfig.getNormalTables();
                logicSchemaConfig.setSchemaName("db1");

                ShardingTableConfig mainSharding = new ShardingTableConfig();
                mainSharding.setCreateTableSQL("CREATE TABLE db1.`sharding` (\n" +
                        "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                        "  `user_id` varchar(100) DEFAULT NULL,\n" +
                        "  `traveldate` date DEFAULT NULL,\n" +
                        "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                        "  `days` int DEFAULT NULL,\n" +
                        "  `blob` longblob,\n" +
                        "  PRIMARY KEY (`id`),\n" +
                        "  KEY `id` (`id`)\n" +
                        ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                        + " dbpartition by mod_hash(id) tbpartition by mod_hash(id) tbpartitions 2 dbpartitions 2;");
                mainSharding.setFunction(ShardingFunction.builder().properties(JsonUtil.from("{\n" +
                        "\t\t\t\t\t\"dbNum\":\"2\",\n" +
                        "\t\t\t\t\t\"mappingFormat\":\"prototype/db1_${dbIndex}/sharding_${tableIndex}\",\n" +
                        "\t\t\t\t\t\"tableNum\":\"2\",\n" +
                        "\t\t\t\t\t\"tableMethod\":\"hash(id)\",\n" +
                        "\t\t\t\t\t\"storeNum\":2,\n" +
                        "\t\t\t\t\t\"dbMethod\":\"hash(id)\"\n" +
                        "\t\t\t\t}", Map.class)).build());
                logicSchemaConfig.getShardingTables().put("sharding", mainSharding);

                NormalTableConfig two = new NormalTableConfig();
                two.setCreateTableSQL("CREATE TABLE `normal` (\n" +
                        "  `id` int(11) NOT NULL,\n" +
                        "  `addressname` varchar(20) DEFAULT NULL,\n" +
                        "  PRIMARY KEY (`id`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n");
                two.getLocality().setTargetName("prototype");
                normalTables.put("two",two);


                schemaConfigs.put("db1",logicSchemaConfig);
                MetadataManager metadataManager = MetadataManager.createMetadataManager(schemaConfigs, new PrototypeService());
                return metadataManager.getSchemaMap();
            }
        }, "select * from db1.sharding inner join db1.two on sharding.id = two.id");

        List<SpecificSql> specificSqls = explain.specificSql();
        Assert.assertEquals("MycatView(distribution=[[db1.sharding, db1.two]]) ",explain.dumpPlan());
        Assert.assertTrue(specificSqls.toString().contains("`sharding`     INNER JOIN db1.two AS "));
        System.out.println();
    }
    @Test
    public void testSelectShardingLeftJoinNormalInOneDb() throws Exception {
        Explain explain = parse(new DrdsConst() {
            @Override
            public NameMap<SchemaHandler> schemas() {
                Map<String, LogicSchemaConfig> schemaConfigs=new HashMap<>();
                LogicSchemaConfig logicSchemaConfig = new LogicSchemaConfig();
                Map<String, NormalTableConfig> normalTables = logicSchemaConfig.getNormalTables();
                logicSchemaConfig.setSchemaName("db1");

                ShardingTableConfig mainSharding = new ShardingTableConfig();
                mainSharding.setCreateTableSQL("CREATE TABLE db1.`sharding` (\n" +
                        "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                        "  `user_id` varchar(100) DEFAULT NULL,\n" +
                        "  `traveldate` date DEFAULT NULL,\n" +
                        "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                        "  `days` int DEFAULT NULL,\n" +
                        "  `blob` longblob,\n" +
                        "  PRIMARY KEY (`id`),\n" +
                        "  KEY `id` (`id`)\n" +
                        ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                        + " dbpartition by mod_hash(id) tbpartition by mod_hash(id) tbpartitions 2 dbpartitions 2;");
                mainSharding.setFunction(ShardingFunction.builder().properties(JsonUtil.from("{\n" +
                        "\t\t\t\t\t\"dbNum\":\"2\",\n" +
                        "\t\t\t\t\t\"mappingFormat\":\"prototype/db1_${dbIndex}/sharding_${tableIndex}\",\n" +
                        "\t\t\t\t\t\"tableNum\":\"2\",\n" +
                        "\t\t\t\t\t\"tableMethod\":\"hash(id)\",\n" +
                        "\t\t\t\t\t\"storeNum\":2,\n" +
                        "\t\t\t\t\t\"dbMethod\":\"hash(id)\"\n" +
                        "\t\t\t\t}", Map.class)).build());
                logicSchemaConfig.getShardingTables().put("sharding", mainSharding);

                NormalTableConfig two = new NormalTableConfig();
                two.setCreateTableSQL("CREATE TABLE `normal` (\n" +
                        "  `id` int(11) NOT NULL,\n" +
                        "  `addressname` varchar(20) DEFAULT NULL,\n" +
                        "  PRIMARY KEY (`id`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n");
                two.getLocality().setTargetName("prototype");
                normalTables.put("two",two);


                schemaConfigs.put("db1",logicSchemaConfig);
                MetadataManager metadataManager = MetadataManager.createMetadataManager(schemaConfigs, new PrototypeService());
                return metadataManager.getSchemaMap();
            }
        }, "select * from db1.sharding left join db1.two on sharding.id = two.id");

        List<SpecificSql> specificSqls = explain.specificSql();
        Assert.assertEquals("MycatView(distribution=[[db1.sharding, db1.two]]) ",explain.dumpPlan());
        Assert.assertTrue(specificSqls.toString().contains("`sharding`     LEFT JOIN db1.two AS "));
        System.out.println();
    }
    @Test
    public void testSelectShardingRightJoinNormalInOneDb() throws Exception {
        Explain explain = parse(new DrdsConst() {
            @Override
            public NameMap<SchemaHandler> schemas() {
                Map<String, LogicSchemaConfig> schemaConfigs=new HashMap<>();
                LogicSchemaConfig logicSchemaConfig = new LogicSchemaConfig();
                Map<String, NormalTableConfig> normalTables = logicSchemaConfig.getNormalTables();
                logicSchemaConfig.setSchemaName("db1");

                ShardingTableConfig mainSharding = new ShardingTableConfig();
                mainSharding.setCreateTableSQL("CREATE TABLE db1.`sharding` (\n" +
                        "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                        "  `user_id` varchar(100) DEFAULT NULL,\n" +
                        "  `traveldate` date DEFAULT NULL,\n" +
                        "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                        "  `days` int DEFAULT NULL,\n" +
                        "  `blob` longblob,\n" +
                        "  PRIMARY KEY (`id`),\n" +
                        "  KEY `id` (`id`)\n" +
                        ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                        + " dbpartition by mod_hash(id) tbpartition by mod_hash(id) tbpartitions 2 dbpartitions 2;");
                mainSharding.setFunction(ShardingFunction.builder().properties(JsonUtil.from("{\n" +
                        "\t\t\t\t\t\"dbNum\":\"2\",\n" +
                        "\t\t\t\t\t\"mappingFormat\":\"prototype/db1_${dbIndex}/sharding_${tableIndex}\",\n" +
                        "\t\t\t\t\t\"tableNum\":\"2\",\n" +
                        "\t\t\t\t\t\"tableMethod\":\"hash(id)\",\n" +
                        "\t\t\t\t\t\"storeNum\":2,\n" +
                        "\t\t\t\t\t\"dbMethod\":\"hash(id)\"\n" +
                        "\t\t\t\t}", Map.class)).build());
                logicSchemaConfig.getShardingTables().put("sharding", mainSharding);

                NormalTableConfig two = new NormalTableConfig();
                two.setCreateTableSQL("CREATE TABLE `normal` (\n" +
                        "  `id` int(11) NOT NULL,\n" +
                        "  `addressname` varchar(20) DEFAULT NULL,\n" +
                        "  PRIMARY KEY (`id`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n");
                two.getLocality().setTargetName("prototype");
                normalTables.put("two",two);


                schemaConfigs.put("db1",logicSchemaConfig);
                MetadataManager metadataManager = MetadataManager.createMetadataManager(schemaConfigs, new PrototypeService());
                return metadataManager.getSchemaMap();
            }
        }, "select * from db1.sharding right join db1.two on sharding.id = two.id");

        List<SpecificSql> specificSqls = explain.specificSql();
        Assert.assertEquals("MycatHashJoin(condition=[=($0, $6)], joinType=[right])   MycatView(distribution=[[db1.sharding]])   MycatView(distribution=[[db1.two]]) ",explain.dumpPlan());
        System.out.println();
    }

    @Test
    public void testSelectShardingRightJoinPartitionKeyNormalInOneDb() throws Exception {
        Explain explain = parse(new DrdsConst() {
            @Override
            public NameMap<SchemaHandler> schemas() {
                Map<String, LogicSchemaConfig> schemaConfigs=new HashMap<>();
                LogicSchemaConfig logicSchemaConfig = new LogicSchemaConfig();
                Map<String, NormalTableConfig> normalTables = logicSchemaConfig.getNormalTables();
                logicSchemaConfig.setSchemaName("db1");

                ShardingTableConfig mainSharding = new ShardingTableConfig();
                mainSharding.setCreateTableSQL("CREATE TABLE db1.`sharding` (\n" +
                        "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                        "  `user_id` varchar(100) DEFAULT NULL,\n" +
                        "  `traveldate` date DEFAULT NULL,\n" +
                        "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                        "  `days` int DEFAULT NULL,\n" +
                        "  `blob` longblob,\n" +
                        "  PRIMARY KEY (`id`),\n" +
                        "  KEY `id` (`id`)\n" +
                        ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                        + " dbpartition by mod_hash(id) tbpartition by mod_hash(id) tbpartitions 2 dbpartitions 2;");
                mainSharding.setFunction(ShardingFunction.builder().properties(JsonUtil.from("{\n" +
                        "\t\t\t\t\t\"dbNum\":\"2\",\n" +
                        "\t\t\t\t\t\"mappingFormat\":\"prototype/db1_${dbIndex}/sharding_${tableIndex}\",\n" +
                        "\t\t\t\t\t\"tableNum\":\"2\",\n" +
                        "\t\t\t\t\t\"tableMethod\":\"hash(id)\",\n" +
                        "\t\t\t\t\t\"storeNum\":2,\n" +
                        "\t\t\t\t\t\"dbMethod\":\"hash(id)\"\n" +
                        "\t\t\t\t}", Map.class)).build());
                logicSchemaConfig.getShardingTables().put("sharding", mainSharding);

                NormalTableConfig two = new NormalTableConfig();
                two.setCreateTableSQL("CREATE TABLE `normal` (\n" +
                        "  `id` int(11) NOT NULL,\n" +
                        "  `addressname` varchar(20) DEFAULT NULL,\n" +
                        "  PRIMARY KEY (`id`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n");
                two.getLocality().setTargetName("prototype");
                normalTables.put("two",two);


                schemaConfigs.put("db1",logicSchemaConfig);
                MetadataManager metadataManager = MetadataManager.createMetadataManager(schemaConfigs, new PrototypeService());
                return metadataManager.getSchemaMap();
            }
        }, "select * from db1.sharding right join db1.two on sharding.id = 1 and sharding.id = two.id");

        List<SpecificSql> specificSqls = explain.specificSql();
        Assert.assertEquals(
                "[Each(targetName=prototype, sql=SELECT `t0`.`id`, `t0`.`user_id`, `t0`.`traveldate`, `t0`.`fee`, `t0`.`days`, `t0`.`blob`, `two`.`id` AS `id0`, `two`.`addressname` FROM (SELECT `sharding`.`id`, `sharding`.`user_id`, `sharding`.`traveldate`, `sharding`.`fee`, `sharding`.`days`, `sharding`.`blob`, (`sharding`.`id` = ?) AS `=`         FROM db1_1.sharding_1 AS `sharding`         WHERE (`sharding`.`id` = ?)) AS `t0`     RIGHT JOIN db1.two AS `two` ON (`t0`.`id` = `two`.`id`))]",explain.specificSql().toString());
        System.out.println();
    }

    @Test
    public void testSelectShardingRightJoinPartitionKeyGlobal() throws Exception {
        Explain explain = parse(new DrdsConst() {
            @Override
            public NameMap<SchemaHandler> schemas() {
                Map<String, LogicSchemaConfig> schemaConfigs=new HashMap<>();
                LogicSchemaConfig logicSchemaConfig = new LogicSchemaConfig();
                Map<String, NormalTableConfig> normalTables = logicSchemaConfig.getNormalTables();
                logicSchemaConfig.setSchemaName("db1");

                ShardingTableConfig mainSharding = new ShardingTableConfig();
                mainSharding.setCreateTableSQL("CREATE TABLE db1.`sharding` (\n" +
                        "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                        "  `user_id` varchar(100) DEFAULT NULL,\n" +
                        "  `traveldate` date DEFAULT NULL,\n" +
                        "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                        "  `days` int DEFAULT NULL,\n" +
                        "  `blob` longblob,\n" +
                        "  PRIMARY KEY (`id`),\n" +
                        "  KEY `id` (`id`)\n" +
                        ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                        + " dbpartition by mod_hash(id) tbpartition by mod_hash(id) tbpartitions 2 dbpartitions 2;");
                mainSharding.setFunction(ShardingFunction.builder().properties(JsonUtil.from("{\n" +
                        "\t\t\t\t\t\"dbNum\":\"2\",\n" +
                        "\t\t\t\t\t\"mappingFormat\":\"prototype/db1_${dbIndex}/sharding_${tableIndex}\",\n" +
                        "\t\t\t\t\t\"tableNum\":\"2\",\n" +
                        "\t\t\t\t\t\"tableMethod\":\"hash(id)\",\n" +
                        "\t\t\t\t\t\"storeNum\":2,\n" +
                        "\t\t\t\t\t\"dbMethod\":\"hash(id)\"\n" +
                        "\t\t\t\t}", Map.class)).build());
                logicSchemaConfig.getShardingTables().put("sharding", mainSharding);

                GlobalTableConfig two = new GlobalTableConfig();
                two.setCreateTableSQL("CREATE TABLE `normal` (\n" +
                        "  `id` int(11) NOT NULL,\n" +
                        "  `addressname` varchar(20) DEFAULT NULL,\n" +
                        "  PRIMARY KEY (`id`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n");
                GlobalBackEndTableInfoConfig globalBackEndTableInfoConfig = new GlobalBackEndTableInfoConfig();
                globalBackEndTableInfoConfig.setTargetName("prototype");
                two.getBroadcast().add(globalBackEndTableInfoConfig);
                logicSchemaConfig.getGlobalTables().put("two",two);


                schemaConfigs.put("db1",logicSchemaConfig);
                MetadataManager metadataManager = MetadataManager.createMetadataManager(schemaConfigs, new PrototypeService());
                return metadataManager.getSchemaMap();
            }
        }, "select * from db1.sharding right join db1.two on sharding.id = 1 and sharding.id = two.id");

        List<SpecificSql> specificSqls = explain.specificSql();
        Assert.assertEquals(
                "[Each(targetName=prototype, sql=SELECT `t0`.`id`, `t0`.`user_id`, `t0`.`traveldate`, `t0`.`fee`, `t0`.`days`, `t0`.`blob`, `two`.`id` AS `id0`, `two`.`addressname` FROM (SELECT `sharding`.`id`, `sharding`.`user_id`, `sharding`.`traveldate`, `sharding`.`fee`, `sharding`.`days`, `sharding`.`blob`, (`sharding`.`id` = ?) AS `=`         FROM db1_1.sharding_1 AS `sharding`         WHERE (`sharding`.`id` = ?)) AS `t0`     RIGHT JOIN db1.two AS `two` ON (`t0`.`id` = `two`.`id`))]",explain.specificSql().toString());
        System.out.println();
    }
    @Test
    public void testSelectShardingRightJoinPartitionKeySharding() throws Exception {
        Explain explain = parse(new DrdsConst() {
            @Override
            public NameMap<SchemaHandler> schemas() {
                Map<String, LogicSchemaConfig> schemaConfigs=new HashMap<>();
                LogicSchemaConfig logicSchemaConfig = new LogicSchemaConfig();
                Map<String, NormalTableConfig> normalTables = logicSchemaConfig.getNormalTables();
                logicSchemaConfig.setSchemaName("db1");

                ShardingTableConfig mainSharding = new ShardingTableConfig();
                mainSharding.setCreateTableSQL("CREATE TABLE db1.`sharding` (\n" +
                        "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                        "  `user_id` varchar(100) DEFAULT NULL,\n" +
                        "  `traveldate` date DEFAULT NULL,\n" +
                        "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                        "  `days` int DEFAULT NULL,\n" +
                        "  `blob` longblob,\n" +
                        "  PRIMARY KEY (`id`),\n" +
                        "  KEY `id` (`id`)\n" +
                        ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                        + " dbpartition by mod_hash(id) tbpartition by mod_hash(id) tbpartitions 2 dbpartitions 2;");
                mainSharding.setFunction(ShardingFunction.builder().properties(JsonUtil.from("{\n" +
                        "\t\t\t\t\t\"dbNum\":\"2\",\n" +
                        "\t\t\t\t\t\"mappingFormat\":\"c_${dbIndex}/db1/sharding_${tableIndex}\",\n" +
                        "\t\t\t\t\t\"tableNum\":\"2\",\n" +
                        "\t\t\t\t\t\"tableMethod\":\"hash(id)\",\n" +
                        "\t\t\t\t\t\"storeNum\":2,\n" +
                        "\t\t\t\t\t\"dbMethod\":\"hash(id)\"\n" +
                        "\t\t\t\t}", Map.class)).build());
                logicSchemaConfig.getShardingTables().put("sharding", mainSharding);

                ShardingTableConfig mainSharding2 = new ShardingTableConfig();
                mainSharding2.setCreateTableSQL("CREATE TABLE db1.`two` (\n" +
                        "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                        "  `user_id` varchar(100) DEFAULT NULL,\n" +
                        "  `traveldate` date DEFAULT NULL,\n" +
                        "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                        "  `days` int DEFAULT NULL,\n" +
                        "  `blob` longblob,\n" +
                        "  PRIMARY KEY (`id`),\n" +
                        "  KEY `id` (`id`)\n" +
                        ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                        + " dbpartition by mod_hash(id) tbpartition by mod_hash(id) tbpartitions 1 dbpartitions 2;");
                mainSharding2.setFunction(ShardingFunction.builder().properties(JsonUtil.from("{\n" +
                        "\t\t\t\t\t\"dbNum\":\"2\",\n" +
                        "\t\t\t\t\t\"mappingFormat\":\"c_${dbIndex}/db1/sharding2\",\n" +
                        "\t\t\t\t\t\"tableNum\":\"1\",\n" +
                        "\t\t\t\t\t\"tableMethod\":\"hash(id)\",\n" +
                        "\t\t\t\t\t\"storeNum\":2,\n" +
                        "\t\t\t\t\t\"dbMethod\":\"hash(id)\"\n" +
                        "\t\t\t\t}", Map.class)).build());
                logicSchemaConfig.getShardingTables().put("two", mainSharding2);


                schemaConfigs.put("db1",logicSchemaConfig);
                MetadataManager metadataManager = MetadataManager.createMetadataManager(schemaConfigs, new PrototypeService());
                return metadataManager.getSchemaMap();
            }
        }, "select * from db1.sharding right join db1.two on sharding.id = 1 and sharding.id = two.id");

        List<SpecificSql> specificSqls = explain.specificSql();
        Assert.assertEquals(
                "[Each(targetName=c_1, sql=SELECT `t0`.`id`, `t0`.`user_id`, `t0`.`traveldate`, `t0`.`fee`, `t0`.`days`, `t0`.`blob`, `two`.`id` AS `id0`, `two`.`user_id` AS `user_id0`, `two`.`traveldate` AS `traveldate0`, `two`.`fee` AS `fee0`, `two`.`days` AS `days0`, `two`.`blob` AS `blob0` FROM (SELECT `sharding`.`id`, `sharding`.`user_id`, `sharding`.`traveldate`, `sharding`.`fee`, `sharding`.`days`, `sharding`.`blob`, (`sharding`.`id` = ?) AS `=`         FROM db1.sharding_1 AS `sharding`         WHERE (`sharding`.`id` = ?)) AS `t0`     RIGHT JOIN db1.sharding2 AS `two` ON (`t0`.`id` = `two`.`id`))]",explain.specificSql().toString());
        System.out.println();
    }
    @Test
    public void testSelectShardingPartitionKeyNormalInOneDb() throws Exception {
        Explain explain = parse(new DrdsConst() {
            @Override
            public NameMap<SchemaHandler> schemas() {
                Map<String, LogicSchemaConfig> schemaConfigs=new HashMap<>();
                LogicSchemaConfig logicSchemaConfig = new LogicSchemaConfig();
                Map<String, NormalTableConfig> normalTables = logicSchemaConfig.getNormalTables();
                logicSchemaConfig.setSchemaName("db1");

                ShardingTableConfig mainSharding = new ShardingTableConfig();
                mainSharding.setCreateTableSQL("CREATE TABLE db1.`sharding` (\n" +
                        "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                        "  `user_id` varchar(100) DEFAULT NULL,\n" +
                        "  `traveldate` date DEFAULT NULL,\n" +
                        "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                        "  `days` int DEFAULT NULL,\n" +
                        "  `blob` longblob,\n" +
                        "  PRIMARY KEY (`id`),\n" +
                        "  KEY `id` (`id`)\n" +
                        ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                        + " dbpartition by mod_hash(id) tbpartition by mod_hash(id) tbpartitions 2 dbpartitions 2;");
                mainSharding.setFunction(ShardingFunction.builder().properties(JsonUtil.from("{\n" +
                        "\t\t\t\t\t\"dbNum\":\"2\",\n" +
                        "\t\t\t\t\t\"mappingFormat\":\"prototype/db1_${dbIndex}/sharding_${tableIndex}\",\n" +
                        "\t\t\t\t\t\"tableNum\":\"2\",\n" +
                        "\t\t\t\t\t\"tableMethod\":\"hash(id)\",\n" +
                        "\t\t\t\t\t\"storeNum\":2,\n" +
                        "\t\t\t\t\t\"dbMethod\":\"hash(id)\"\n" +
                        "\t\t\t\t}", Map.class)).build());
                logicSchemaConfig.getShardingTables().put("sharding", mainSharding);

                NormalTableConfig two = new NormalTableConfig();
                two.setCreateTableSQL("CREATE TABLE `normal` (\n" +
                        "  `id` int(11) NOT NULL,\n" +
                        "  `addressname` varchar(20) DEFAULT NULL,\n" +
                        "  PRIMARY KEY (`id`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n");
                two.getLocality().setTargetName("prototype");
                normalTables.put("two",two);


                schemaConfigs.put("db1",logicSchemaConfig);
                MetadataManager metadataManager = MetadataManager.createMetadataManager(schemaConfigs, new PrototypeService());
                return metadataManager.getSchemaMap();
            }
        }, "select * from db1.sharding join db1.two on sharding.id = two.id");

        List<SpecificSql> specificSqls = explain.specificSql();
        Assert.assertEquals("MycatView(distribution=[[db1.sharding, db1.two]]) ",explain.dumpPlan());
        Assert.assertTrue(specificSqls.toString().contains("`sharding`     INNER JOIN db1.two AS "));
        System.out.println();
    }

    @Test
    public void testSelectNormalGlobal() throws Exception {
        Explain explain = parse("select * from db1.normal s join db1.global e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=addressname}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[[db1.global, db1.normal]]) ", explain.dumpPlan());
//        Assert.assertEquals("[SpecificSql(relNode=MycatView(distribution=[normalTables=db1.normal,globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.normal      INNER JOIN db1.global ON (`normal`.`id` = `global`.`id`), sqls=[Each(targetName=prototype, sql=SELECT * FROM db1.normal     INNER JOIN db1.global ON (`normal`.`id` = `global`.`id`))])]",
//                explain.specificSql().toString());
    }

    @Test
    public void testSelectSharding() throws Exception {
        Explain explain = parse("select * from db1.sharding");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[[db1.sharding]]) ", explain.dumpPlan());
//        Assert.assertEquals(
//                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding]) , parameterizedSql=SELECT *  FROM db1.sharding, sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.sharding_0     UNION ALL     SELECT *     FROM db1_0.sharding_1)), Each(targetName=c1, sql=(SELECT *     FROM db1_1.sharding_0     UNION ALL     SELECT *     FROM db1_1.sharding_1))])]",
//                explain.specificSql().toString());
    }

    @Test
    public void testSelectShardingWhere() throws Exception {
        Explain explain = parse("select * from db1.sharding where id = 1");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[[db1.sharding]], conditions=[=($0, CAST(?0):BIGINT NOT NULL)]) ", explain.dumpPlan());
//        Assert.assertEquals(
//                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding], conditions=[=(CAST($0):DECIMAL(19, 0), ?0)]) , parameterizedSql=SELECT *  FROM db1.sharding  WHERE (CAST(`id` AS decimal) = ?), sqls=[Each(targetName=c0, sql=SELECT * FROM db1_0.sharding_1 WHERE (CAST(`id` AS decimal) = ?))])]",
//                explain.specificSql().toString());
    }

    @Test
    public void testSelectShardingSelf() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.sharding e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=user_id0}, {columnType=DATE, nullable=true, columnName=traveldate0}, {columnType=DECIMAL, nullable=true, columnName=fee0}, {columnType=BIGINT, nullable=true, columnName=days0}, {columnType=VARBINARY, nullable=true, columnName=blob0}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[[db1.sharding, db1.sharding]]) ", explain.dumpPlan());
//        Assert.assertEquals("[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding,db1.sharding]) , parameterizedSql=SELECT *  FROM db1.sharding      INNER JOIN db1.sharding AS `sharding0` ON (`sharding`.`id` = `sharding0`.`id`), sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.sharding_0         INNER JOIN db1_0.sharding_0 AS `sharding_00` ON (`sharding_0`.`id` = `sharding_00`.`id`)     UNION ALL     SELECT *     FROM db1_0.sharding_1         INNER JOIN db1_0.sharding_1 AS `sharding_10` ON (`sharding_1`.`id` = `sharding_10`.`id`))), Each(targetName=c1, sql=(SELECT *     FROM db1_1.sharding_0         INNER JOIN db1_1.sharding_0 AS `sharding_00` ON (`sharding_0`.`id` = `sharding_00`.`id`)     UNION ALL     SELECT *     FROM db1_1.sharding_1         INNER JOIN db1_1.sharding_1 AS `sharding_10` ON (`sharding_1`.`id` = `sharding_10`.`id`)))])]",
//                explain.specificSql().toString());
    }


    @Test
    public void testSelectShardingGlobal() throws Exception {
        Explain explain = parse("select * from db1.sharding s join db1.global e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[[db1.global, db1.sharding]]) ", explain.dumpPlan());
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
        Assert.assertEquals("MycatView(distribution=[[db1.er, db1.sharding]], conditions=[=($0, CAST(?0):BIGINT NOT NULL)]) ", explain.dumpPlan());
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
        Assert.assertEquals("MycatView(distribution=[[db1.er, db1.sharding]]) ", explain.dumpPlan());
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
        Assert.assertTrue(explain.dumpPlan().contains("MycatView(distribution=[[db1.er, db1.global, db1.sharding]]) "));
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
        Assert.assertTrue(explain.dumpPlan().contains("MycatView(distribution=[[db1.er, db1.global, db1.sharding]]) "));
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
        Assert.assertTrue(explain.dumpPlan().contains("MycatView(distribution=[[db1.er, db1.global, db1.sharding]]) "));
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
        Assert.assertTrue(explain.dumpPlan().contains("MycatView(distribution=[[db1.er, db1.global, db1.sharding]]) "));
        System.out.println(explain.specificSql());
//        Assert.assertEquals(
//                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding,globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.global      INNER JOIN db1.sharding ON (`global`.`id` = `sharding`.`id`), sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1.global         INNER JOIN db1_0.sharding_0 ON (`global`.`id` = `sharding_0`.`id`)     UNION ALL     SELECT *     FROM db1.global AS `global0`         INNER JOIN db1_0.sharding_1 ON (`global0`.`id` = `sharding_1`.`id`))), Each(targetName=c1, sql=(SELECT *     FROM db1.global         INNER JOIN db1_1.sharding_0 ON (`global`.`id` = `sharding_0`.`id`)     UNION ALL     SELECT *     FROM db1.global AS `global0`         INNER JOIN db1_1.sharding_1 ON (`global0`.`id` = `sharding_1`.`id`)))]), SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.er]) , parameterizedSql=SELECT *  FROM db1.er, sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1_0.er_0     UNION ALL     SELECT *     FROM db1_0.er_1)), Each(targetName=c1, sql=(SELECT *     FROM db1_1.er_0     UNION ALL     SELECT *     FROM db1_1.er_1))])]",
//                explain.specificSql().toString());
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
                        "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}]")
        );

        Assert.assertEquals("MycatView(distribution=[[db1.global]]) ", explain.dumpPlan());
//        Assert.assertEquals("[[SpecificSql(relNode=MycatView(distribution=[globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.global, sqls=[Each(targetName=c0, sql=SELECT * FROM db1.global)])], [SpecificSql(relNode=MycatView(distribution=[globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.global, sqls=[Each(targetName=c1, sql=SELECT * FROM db1.global)])]]",
//                sqlSet.toString());

    }

    @Test
    public void testSelectGlobalNormal() throws Exception {
        Explain explain = parse("select * from db1.global s join db1.normal e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=addressname}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[[db1.global, db1.normal]]) ", explain.dumpPlan());
//        Assert.assertEquals(
//                "[SpecificSql(relNode=MycatView(distribution=[normalTables=db1.normal,globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.global      INNER JOIN db1.normal ON (`global`.`id` = `normal`.`id`), sqls=[Each(targetName=prototype, sql=SELECT * FROM db1.global     INNER JOIN db1.normal ON (`global`.`id` = `normal`.`id`))])]",
//                explain.specificSql().toString());

    }

    @Test
    public void testSelectGlobalSharding() throws Exception {
        Explain explain = parse("select * from db1.global s join db1.sharding e on s.id = e.id");
        Assert.assertEquals(
                "[{columnType=BIGINT, nullable=false, columnName=id}, {columnType=VARCHAR, nullable=true, columnName=companyname}, {columnType=BIGINT, nullable=true, columnName=addressid}, {columnType=BIGINT, nullable=false, columnName=id0}, {columnType=VARCHAR, nullable=true, columnName=user_id}, {columnType=DATE, nullable=true, columnName=traveldate}, {columnType=DECIMAL, nullable=true, columnName=fee}, {columnType=BIGINT, nullable=true, columnName=days}, {columnType=VARBINARY, nullable=true, columnName=blob}]",
                explain.getColumnInfo());
        Assert.assertEquals("MycatView(distribution=[[db1.global, db1.sharding]]) ", explain.dumpPlan());
//        Assert.assertEquals(
//                "[SpecificSql(relNode=MycatView(distribution=[shardingTables=db1.sharding,globalTables=db1.global]) , parameterizedSql=SELECT *  FROM db1.global      INNER JOIN db1.sharding ON (`global`.`id` = `sharding`.`id`), sqls=[Each(targetName=c0, sql=(SELECT *     FROM db1.global         INNER JOIN db1_0.sharding_0 ON (`global`.`id` = `sharding_0`.`id`)     UNION ALL     SELECT *     FROM db1.global AS `global0`         INNER JOIN db1_0.sharding_1 ON (`global0`.`id` = `sharding_1`.`id`))), Each(targetName=c1, sql=(SELECT *     FROM db1.global         INNER JOIN db1_1.sharding_0 ON (`global`.`id` = `sharding_0`.`id`)     UNION ALL     SELECT *     FROM db1.global AS `global0`         INNER JOIN db1_1.sharding_1 ON (`global0`.`id` = `sharding_1`.`id`)))])]",
//                explain.specificSql().toString());

    }



}
