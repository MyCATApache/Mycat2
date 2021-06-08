package io.mycat.drdsrunner;

import io.mycat.*;
import io.mycat.assemble.MycatTest;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.calcite.MycatRel;
import io.mycat.calcite.rewriter.OptimizationContext;
import io.mycat.calcite.spm.Plan;
import io.mycat.calcite.spm.PlanImpl;
import io.mycat.config.*;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.runtime.MycatDataContextImpl;
import io.mycat.util.JsonUtil;
import lombok.SneakyThrows;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Util;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;


public abstract class DrdsTest implements MycatTest {

    static DrdsSqlCompiler drdsRunner = null;
    static MetadataManager metadataManager;

    @SneakyThrows
    public static DrdsSqlCompiler getDrds() {
        if (drdsRunner != null) {
            return drdsRunner;
        }
        synchronized (DrdsTest.class) {
            if (drdsRunner == null) {
                System.setProperty("mode","local");
                MycatCore mycatCore = new MycatCore();
                FileMetadataStorageManager fileMetadataStorageManager = MetaClusterCurrent.wrapper(FileMetadataStorageManager.class);
                MycatRouterConfig mycatRouterConfig = new MycatRouterConfig();
                LogicSchemaConfig logicSchemaConfig = new LogicSchemaConfig();
                mycatRouterConfig.getSchemas().add(logicSchemaConfig);
                logicSchemaConfig.setSchemaName("db1");

                NormalTableConfig mainNormalTableConfig = new NormalTableConfig();
                mainNormalTableConfig.setCreateTableSQL("CREATE TABLE `normal` (\n" +
                        "  `id` int(11) NOT NULL,\n" +
                        "  `addressname` varchar(20) DEFAULT NULL,\n" +
                        "  PRIMARY KEY (`id`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n");
                NormalBackEndTableInfoConfig normalBackEndTableInfoConfig = new NormalBackEndTableInfoConfig();
                normalBackEndTableInfoConfig.setTargetName("prototype");
                normalBackEndTableInfoConfig.setSchemaName("db1");
                normalBackEndTableInfoConfig.setTableName("normal");
                mainNormalTableConfig.setDataNode(normalBackEndTableInfoConfig);
                logicSchemaConfig.getNormalTables().put("normal", mainNormalTableConfig);

                NormalTableConfig orherNormalTableConfig = new NormalTableConfig();
                orherNormalTableConfig.setCreateTableSQL("CREATE TABLE `normal2` (\n" +
                        "  `id` int(11) NOT NULL,\n" +
                        "  `addressname` varchar(20) DEFAULT NULL,\n" +
                        "  PRIMARY KEY (`id`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n");
                NormalBackEndTableInfoConfig otherNormalBackEndTableInfoConfig = new NormalBackEndTableInfoConfig();
                otherNormalBackEndTableInfoConfig.setTargetName("prototype");
                otherNormalBackEndTableInfoConfig.setSchemaName("db1");
                otherNormalBackEndTableInfoConfig.setTableName("normal2");
                orherNormalTableConfig.setDataNode(otherNormalBackEndTableInfoConfig);
                logicSchemaConfig.getNormalTables().put("normal2", orherNormalTableConfig);

                GlobalTableConfig globalTableConfig = new GlobalTableConfig();
                globalTableConfig.getDataNodes().add(
                        GlobalBackEndTableInfoConfig.builder().targetName("c0").build()
                        );
                globalTableConfig.getDataNodes().add(
                        GlobalBackEndTableInfoConfig.builder().targetName("c1").build()
                        );
                globalTableConfig.setCreateTableSQL("CREATE TABLE `global` (\n" +
                        "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                        "  `companyname` varchar(20) DEFAULT NULL,\n" +
                        "  `addressid` int(11) DEFAULT NULL,\n" +
                        "  PRIMARY KEY (`id`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 broadcast; ");
                logicSchemaConfig.getGlobalTables().put("global", globalTableConfig);

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
                mainSharding.setFunction(ShardingFuntion.builder().properties(JsonUtil.from("{\n" +
                        "\t\t\t\t\t\"dbNum\":\"2\",\n" +
                        "\t\t\t\t\t\"mappingFormat\":\"c${targetIndex}/db1_${dbIndex}/sharding_${tableIndex}\",\n" +
                        "\t\t\t\t\t\"tableNum\":\"2\",\n" +
                        "\t\t\t\t\t\"tableMethod\":\"hash(id)\",\n" +
                        "\t\t\t\t\t\"storeNum\":2,\n" +
                        "\t\t\t\t\t\"dbMethod\":\"hash(id)\"\n" +
                        "\t\t\t\t}", Map.class)).build());
                logicSchemaConfig.getShadingTables().put("sharding", mainSharding);

                ShardingTableConfig er = new ShardingTableConfig();
                er.setCreateTableSQL("CREATE TABLE db1.`er` (\n" +
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
                er.setFunction(ShardingFuntion.builder().properties(JsonUtil.from("{\n" +
                        "\t\t\t\t\t\"dbNum\":\"2\",\n" +
                        "\t\t\t\t\t\"mappingFormat\":\"c${targetIndex}/db1_${dbIndex}/er_${tableIndex}\",\n" +
                        "\t\t\t\t\t\"tableNum\":\"2\",\n" +
                        "\t\t\t\t\t\"tableMethod\":\"hash(id)\",\n" +
                        "\t\t\t\t\t\"storeNum\":2,\n" +
                        "\t\t\t\t\t\"dbMethod\":\"hash(id)\"\n" +
                        "\t\t\t\t}", Map.class)).build());
                logicSchemaConfig.getShadingTables().put("er", er);

                ShardingTableConfig other_sharding = new ShardingTableConfig();
                other_sharding.setCreateTableSQL("CREATE TABLE db1.`other_sharding` (\n" +
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
                other_sharding.setFunction(ShardingFuntion.builder().properties(JsonUtil.from("{\n" +
                        "\t\t\t\t\t\"dbNum\":\"2\",\n" +
                        "\t\t\t\t\t\"mappingFormat\":\"c${targetIndex}/db1_${dbIndex}/other_sharding_${tableIndex}\",\n" +
                        "\t\t\t\t\t\"tableNum\":\"2\",\n" +
                        "\t\t\t\t\t\"tableMethod\":\"UNI_HASH(id)\",\n" +
                        "\t\t\t\t\t\"storeNum\":2,\n" +
                        "\t\t\t\t\t\"dbMethod\":\"hash(id)\"\n" +
                        "\t\t\t\t}", Map.class)).build());
                logicSchemaConfig.getShadingTables().put("other_sharding", other_sharding);

                mycatRouterConfig.getClusters().add(CreateClusterHint.createConfig("c0", Arrays.asList("ds0"), Collections.emptyList()));
                mycatRouterConfig.getClusters().add(CreateClusterHint.createConfig("c1", Arrays.asList("ds1"), Collections.emptyList()));

                {
                    NormalTableConfig orherTargetNormalTableConfig = new NormalTableConfig();
                    orherTargetNormalTableConfig.setCreateTableSQL("CREATE TABLE `normal3` (\n" +
                            "  `id` int(11) NOT NULL,\n" +
                            "  `addressname` varchar(20) DEFAULT NULL,\n" +
                            "  PRIMARY KEY (`id`)\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n");
                    NormalBackEndTableInfoConfig otherTargetNormalBackEndTableInfoConfig = new NormalBackEndTableInfoConfig();
                    otherTargetNormalBackEndTableInfoConfig.setTargetName("ds1");
                    otherTargetNormalBackEndTableInfoConfig.setSchemaName("db1");
                    otherTargetNormalBackEndTableInfoConfig.setTableName("normal3");
                    orherTargetNormalTableConfig.setDataNode(otherTargetNormalBackEndTableInfoConfig);
                    logicSchemaConfig.getNormalTables().put("normal3", orherTargetNormalTableConfig);

                }


                mycatRouterConfig.getDatasources().add(CreateDataSourceHint.createConfig("ds0", DB1));
                mycatRouterConfig.getDatasources().add(CreateDataSourceHint.createConfig("ds1", DB2));
                mycatRouterConfig.getDatasources().add(CreateDataSourceHint.createConfig("prototype", DB1));
                fileMetadataStorageManager.start(mycatRouterConfig);
                drdsRunner = MetaClusterCurrent.wrapper(DrdsSqlCompiler.class);
                metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
            }
        }
        return drdsRunner;
    }


    public static Explain parse(String sql) {
        DrdsSqlCompiler drds = getDrds();
        DrdsSqlWithParams drdsSqlWithParams = DrdsRunnerHelper.preParse(sql, null);
        OptimizationContext optimizationContext = new OptimizationContext();
        MycatRel dispatch = drds.dispatch(optimizationContext, drdsSqlWithParams);
        Plan plan = new PlanImpl(dispatch, DrdsExecutorCompiler.getCodeExecuterContext(dispatch,false), drdsSqlWithParams.getAliasList());
        return new Explain(plan,drdsSqlWithParams);
    }

    public static MetadataManager getMetadataManager() {
        return metadataManager;
    }

    public static String dumpPlan(RelNode relNode) {
        String dumpPlan = Util.toLinux(RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT,
                SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        System.out.println(dumpPlan);
        return dumpPlan;
    }
}
