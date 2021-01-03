package io.mycat.assemble;

import io.mycat.config.GlobalBackEndTableInfoConfig;
import io.mycat.config.ShardingBackEndTableInfoConfig;
import io.mycat.config.ShardingFuntion;
import io.mycat.hint.*;
import io.mycat.router.mycat1xfunction.PartitionConstant;
import org.apache.groovy.util.Maps;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class DDLHintTest implements MycatTest {

    @Test
    public void testShowDataNodes() throws Exception {
        try (Connection mycat = getMySQLConnection(DB_MYCAT);) {
            String db = "db1";
            execute(mycat, RESET_CONFIG);
            execute(mycat, "create database " + db);
            execute(mycat, "use " + db);

            execute(
                    mycat,
                    CreateTableHint
                            .createNormal("db1", "normal", "create table normal(id int)", "prototype")
            );
            List<Map<String, Object>> maps = executeQuery(mycat, ShowDataNodeHint.create("db1", "normal"));
            Assert.assertTrue(maps.size() > 0);


            execute(
                    mycat,
                    CreateTableHint
                            .createGlobal(db, "global", "create table global(id int)", Arrays.asList(
                                    GlobalBackEndTableInfoConfig.builder().targetName("prototype").build()))
            );

            maps = executeQuery(mycat, ShowDataNodeHint.create("db1", "global"));
            Assert.assertTrue(maps.size() > 0);

            execute(
                    mycat,
                    CreateTableHint
                            .createSharding(db, "sharding", "create table sharding(id int)",
                                    ShardingBackEndTableInfoConfig.builder()
                                            .schemaNames(db)
                                            .tableNames("sharding")
                                            .targetNames("prototype").build(),
                                    ShardingFuntion.builder().clazz(PartitionConstant.class.getCanonicalName())
                                            .properties(Maps.of("defaultNode", "0", "columnName", "id")).build())
            );

            maps = executeQuery(mycat, ShowDataNodeHint.create("db1", "sharding"));
            Assert.assertTrue(maps.size() > 0);
        }
    }

    @Test
    public void testCreateTable() throws Exception {
        try (Connection mycat = getMySQLConnection(DB_MYCAT);) {
            execute(mycat, RESET_CONFIG);
            String db = "testSchema";
            execute(mycat, "drop database " + db);
            execute(mycat, "create database " + db);
            execute(mycat, "use " + db);

            execute(
                    mycat,
                    CreateTableHint
                            .createNormal(db, "normal", "create table normal(id int)", "prototype")
            );
            hasData(mycat, db, "normal");

            execute(
                    mycat,
                    CreateTableHint
                            .createGlobal(db, "global", "create table global(id int)", Arrays.asList(
                                    GlobalBackEndTableInfoConfig.builder().targetName("prototype").build()))
            );
            hasData(mycat, db, "global");
            execute(
                    mycat,
                    CreateTableHint
                            .createSharding(db, "sharding", "create table sharding(id int)",
                                    ShardingBackEndTableInfoConfig.builder()
                                            .schemaNames(db)
                                            .tableNames("sharding")
                                            .targetNames("prototype").build(),
                                    ShardingFuntion.builder().clazz(PartitionConstant.class.getCanonicalName())
                                            .properties(Maps.of("defaultNode", "0", "columnName", "id")).build())
            );
            hasData(mycat, db, "sharding");
            execute(mycat, "drop database " + db);
        }
    }


    @Test
    public void testAddDatasource() throws Exception {
        try (Connection mycat = getMySQLConnection(DB_MYCAT);) {
            execute(mycat, RESET_CONFIG);
            String dsName = "newDs";
            execute(mycat, DropDataSourceHint.create(dsName));
            Assert.assertTrue(
                    !executeQuery(mycat, "/*+ mycat:showDataSources{} */")
                            .toString().contains(dsName));
            execute(mycat, CreateDataSourceHint
                    .create(dsName,
                            DB1));
            Assert.assertTrue(
                    executeQuery(mycat, "/*+ mycat:showDataSources{} */")
                            .toString().contains("newDs"));
            execute(mycat, DropDataSourceHint.create(dsName));
            Assert.assertTrue(
                    !executeQuery(mycat, "/*+ mycat:showDataSources{} */")
                            .toString().contains(dsName));
        }
    }


    @Test
    public void testAddCluster() throws Exception {
        String clusterName = "testAddCluster";
        try (Connection mycat = getMySQLConnection(DB_MYCAT);) {
            execute(mycat, RESET_CONFIG);
            execute(mycat, DropClusterHint.create(clusterName));
            Assert.assertTrue(
                    !executeQuery(mycat, "/*+ mycat:showClusters{} */")
                            .toString().contains(clusterName));
            execute(mycat, CreateDataSourceHint
                    .create("newDs",
                            DB1));
            execute(mycat, CreateClusterHint.create(clusterName, Arrays.asList("newDs"), Collections.emptyList()));
            Assert.assertTrue(
                    executeQuery(mycat, "/*+ mycat:showClusters{} */")
                            .toString().contains(clusterName));
            execute(mycat, DropClusterHint.create(clusterName));

            Assert.assertTrue(
                    !executeQuery(mycat, "/*+ mycat:showClusters{} */")
                            .toString().contains(clusterName));
            execute(mycat, DropDataSourceHint
                    .create("newDs"));
        }

    }

    @Test
    public void testAddSchema() throws Exception {
        try (Connection mycat = getMySQLConnection(DB_MYCAT);
             Connection mysql = getMySQLConnection(DB1);
        ) {
            execute(mycat, RESET_CONFIG);
            String schemaName = "test_add_Schema";
            String tableName = "test_table";
            execute(mysql, "create database  if not exists " + schemaName);
            execute(mysql, "create table if not exists " +
                    schemaName +
                    "." +
                    tableName +
                    " (id bigint) ");

            execute(mycat, CreateSchemaHint.create(schemaName, "prototype"));
            List<Map<String, Object>> shouldContainsTestAddSchema = executeQuery(mycat, "show databases");
            Assert.assertTrue(shouldContainsTestAddSchema.toString().contains(schemaName));
            List<Map<String, Object>> shouldContainsTable = executeQuery(mycat, "show tables from " + schemaName);
            Assert.assertTrue(shouldContainsTable.toString().toLowerCase().contains(tableName.toLowerCase()));
            execute(mycat, "drop database " + schemaName);
            List<Map<String, Object>> shouldNotContainsTestAddSchema = executeQuery(mycat, "show databases");
            Assert.assertFalse(shouldNotContainsTestAddSchema.toString().contains(schemaName));

        }
    }
}
