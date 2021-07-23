package io.mycat.gsi;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.hint.ShowTopologyHint;
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
public class CreateGsiTest implements MycatTest {

    @Test
    public void createGsi() throws Exception {
        initShardingTable();
        try (Connection connection = getMySQLConnection(DB_MYCAT)) {
            execute(connection, "CREATE UNIQUE GLOBAL INDEX `g_i_user_id` ON `db1`.`travelrecord`(`user_id`) \n" +
                    "    COVERING(`fee`,id) \n" +
                    "    dbpartition by mod_hash(`user_id`) tbpartition by mod_hash(`user_id`) dbpartitions 2 tbpartitions 2");
            boolean b = hasData(connection, "db1", "travelrecord_g_i_user_id");//test create it
            List<Map<String, Object>> travelrecord_g_i_user_id_topologyHint = executeQuery(connection, ShowTopologyHint.create("db1", "travelrecord_g_i_user_id").toString());
            Assert.assertEquals("[{targetName=c0, schemaName=db1_0, tableName=travelrecord_g_i_user_id_0}, {targetName=c0, schemaName=db1_0, tableName=travelrecord_g_i_user_id_1}, {targetName=c1, schemaName=db1_1, tableName=travelrecord_g_i_user_id_2}, {targetName=c1, schemaName=db1_1, tableName=travelrecord_g_i_user_id_3}]", travelrecord_g_i_user_id_topologyHint.toString());


            String explainPrimaryTable = explain(connection, "select * from db1.travelrecord where id = 1");
            Assert.assertTrue(explainPrimaryTable.contains("Each(targetName=c0, sql=SELECT * FROM db1_0.travelrecord_1 AS `travelrecord` WHERE (`travelrecord`.`id` = ?))"));
            String explainIndexScan = explain(connection, "select * from db1.travelrecord where user_id = 1");//index-scan
            Assert.assertTrue(explainIndexScan.contains("MycatProject(id=[$0], user_id=[$1], traveldate=[$3], fee=[$2], days=[$4], blob=[$5])\n" +
                    "  MycatSQLTableLookup(condition=[=($0, $7)], joinType=[inner], type=[BACK], correlationIds=[[$cor0]], leftKeys=[[0]])\n" +
                    "    MycatView(distribution=[[db1.travelrecord_g_i_user_id]], conditions=[=($0, ?0)])\n" +
                    "    MycatView(distribution=[[db1.travelrecord]])"));
            String explainOnlyIndexScan = explain(connection, "select fee from db1.travelrecord where user_id = 1");//index-scan
            Assert.assertTrue(explainOnlyIndexScan.contains("Each(targetName=c0, sql=SELECT `travelrecord_g_i_user_id`.`fee` FROM db1_0.travelrecord_g_i_user_id_1 AS `travelrecord_g_i_user_id` WHERE (`travelrecord_g_i_user_id`.`user_id` = ?))"));

            String explain;
            explain = explain(connection, "delete  from db1.travelrecord");
            Assert.assertTrue(explain.contains("travelrecord_g_i_user_id"));
            deleteData(connection, "db1", "travelrecord");
            deleteData(connection, "db1", "travelrecord_g_i_user_id");

            explain = explain(connection, "insert db1.travelrecord (id,user_id) values(1,2)");
            Assert.assertTrue(explain.contains("travelrecord_g_i_user_id"));
            for (int i = 0; i < 10; i++) {
                execute(connection, "insert db1.travelrecord (id,user_id) values(" + i + "," +
                        "" +
                        i +
                        ")");
            }
            List<Map<String, Object>> maps = executeQuery(connection, "select fee from db1.travelrecord where user_id = 1");
            Assert.assertEquals(1, maps.size());
            System.out.println();

            //测试事务
            long count0 = count(connection, "db1", "travelrecord");
            long count1 = count(connection, "db1", "travelrecord_g_i_user_id");

            Assert.assertEquals(count0, count1);
            String explain1 = explain(connection, "insert db1.travelrecord (id,user_id) values(" + 100 + "," +
                    "" +
                    100 +
                    ")");
            Assert.assertTrue(explain1.contains("travelrecord_0") && explain1.contains("travelrecord_g_i_user_id_1"));

            connection.setAutoCommit(false);
            for (int i = 10; i < 20; i++) {
                execute(connection, "insert db1.travelrecord (id,user_id) values(" + i + "," +
                        "" +
                        i +
                        ")");
            }
            connection.rollback();

            long _count0 = count(connection, "db1", "travelrecord");
            long _count1 = count(connection, "db1", "travelrecord_g_i_user_id");
            Assert.assertEquals(count0, _count0);
            Assert.assertEquals(count1, _count1);

            for (int i = 10; i < 20; i++) {
                execute(connection, "insert db1.travelrecord (id,user_id) values(" + i + "," +
                        "" +
                        i +
                        ")");
            }
            connection.setAutoCommit(true);
            _count0 = count(connection, "db1", "travelrecord");
            _count1 = count(connection, "db1", "travelrecord_g_i_user_id");
            Assert.assertEquals(20, _count0);
            Assert.assertEquals(20, _count1);


            testInsertException(connection, TranscationType.XA);
            testInsertException(connection, TranscationType.PROXY);
        }

    }

    enum TranscationType {
        XA, PROXY
    }

    private void testInsertException(Connection connection, TranscationType transcationType) throws Exception {
        String initSQL = transcationType == TranscationType.XA ? "set transaction_policy  = xa" : "set transaction_policy  = proxy";
        long _count1;
        long _count0;
        JdbcUtils.execute(connection, initSQL);

        connection.setAutoCommit(true);
        _count0 = count(connection, "db1", "travelrecord");
        _count1 = count(connection, "db1", "travelrecord_g_i_user_id");
        Assert.assertEquals(_count0, _count1);
        Exception exception = null;
        try {
            execute(connection, "insert db1.travelrecord (id,user_id,fee) values(" + 21 + "," +
                    "" +
                    21 +
                    "," +
                    "1/0" +
                    ")");
        } catch (Exception e) {
            exception = e;
        }
        Assert.assertTrue(exception != null);
//        switch (transcationType) {
//            case XA:
                Assert.assertEquals(_count0, count(connection, "db1", "travelrecord"));
                Assert.assertEquals(_count1, count(connection, "db1", "travelrecord_g_i_user_id"));
//                break;
//            case PROXY:
//                Assert.assertNotEquals(count(connection, "db1", "travelrecord"), count(connection, "db1", "travelrecord_g_i_user_id"));
//                break;
//        }

    }


    @Test
    public void createGsi2() throws Exception {
        initShardingTable();
        try (Connection connection = getMySQLConnection(DB_MYCAT)) {
            execute(connection, "CREATE TABLE IF NOT EXISTS db1.`travelrecord` (\n\t`id` bigint NOT NULL AUTO_INCREMENT,\n\t`user_id` varchar(100) DEFAULT NULL,\n\t`traveldate` date DEFAULT NULL,\n\t`fee` decimal(10, 0) DEFAULT NULL,\n\t`days` int DEFAULT NULL,\n\t`blob` longblob,\n\tPRIMARY KEY (`id`),\n\tKEY `id` (`id`),\n\tGLOBAL INDEX `g_i_user_id`(`user_id`) COVERING (`fee`, id) DBPARTITION BY mod_hash(`user_id`) TBPARTITION BY mod_hash(`user_id`) DBPARTITIONS 2 TBPARTITIONS 2\n) ENGINE = InnoDB CHARSET = utf8\nDBPARTITION BY mod_hash(id) DBPARTITIONS 2\nTBPARTITION BY mod_hash(id) TBPARTITIONS 2");
            boolean b = hasData(connection, "db1", "travelrecord_g_i_user_id");//test create it
            List<Map<String, Object>> travelrecord_g_i_user_id_topologyHint = executeQuery(connection, ShowTopologyHint.create("db1", "travelrecord_g_i_user_id").toString());
            Assert.assertEquals("[{targetName=c0, schemaName=db1_0, tableName=travelrecord_g_i_user_id_0}, {targetName=c0, schemaName=db1_0, tableName=travelrecord_g_i_user_id_1}, {targetName=c1, schemaName=db1_1, tableName=travelrecord_g_i_user_id_2}, {targetName=c1, schemaName=db1_1, tableName=travelrecord_g_i_user_id_3}]", travelrecord_g_i_user_id_topologyHint.toString());


            String explainPrimaryTable = explain(connection, "select * from db1.travelrecord where id = 1");
            Assert.assertTrue(explainPrimaryTable.contains("Each(targetName=c0, sql=SELECT * FROM db1_0.travelrecord_1 AS `travelrecord` WHERE (`travelrecord`.`id` = ?))"));
            String explainIndexScan = explain(connection, "select * from db1.travelrecord where user_id = 1");//index-scan
            Assert.assertTrue(explainIndexScan.contains("MycatProject(id=[$0], user_id=[$1], traveldate=[$3], fee=[$2], days=[$4], blob=[$5])\n" +
                    "  MycatSQLTableLookup(condition=[=($0, $7)], joinType=[inner], type=[BACK], correlationIds=[[$cor0]], leftKeys=[[0]])\n" +
                    "    MycatView(distribution=[[db1.travelrecord_g_i_user_id]], conditions=[=($0, ?0)])\n" +
                    "    MycatView(distribution=[[db1.travelrecord]])"));
            String explainOnlyIndexScan = explain(connection, "select fee from db1.travelrecord where user_id = 1");//index-scan
            Assert.assertTrue(explainOnlyIndexScan.contains("Each(targetName=c0, sql=SELECT `travelrecord_g_i_user_id`.`fee` FROM db1_0.travelrecord_g_i_user_id_1 AS `travelrecord_g_i_user_id` WHERE (`travelrecord_g_i_user_id`.`user_id` = ?))"));
            deleteData(connection, "db1", "travelrecord");
            deleteData(connection, "db1", "travelrecord_g_i_user_id");
            for (int i = 1; i < 10; i++) {
                execute(connection, "insert db1.travelrecord (id,user_id) values(" + i + "," +
                        "" +
                        i +
                        ")");
            }
            List<Map<String, Object>> maps = executeQuery(connection, "select fee from db1.travelrecord where user_id = 1");
            Assert.assertEquals(1, maps.size());
            System.out.println();

        }

    }

    private void initShardingTable() throws Exception {
        Connection mycatConnection = getMySQLConnection(DB_MYCAT);
        execute(mycatConnection, RESET_CONFIG);

        execute(mycatConnection, "DROP DATABASE db1");


        execute(mycatConnection, "CREATE DATABASE db1");

        execute(mycatConnection, CreateDataSourceHint
                .create("ds0",
                        DB1));
        execute(mycatConnection, CreateDataSourceHint
                .create("ds1",
                        DB2));

        execute(mycatConnection,
                CreateClusterHint.create("c0",
                        Arrays.asList("ds0"), Collections.emptyList()));
        execute(mycatConnection,
                CreateClusterHint.create("c1",
                        Arrays.asList("ds1"), Collections.emptyList()));

        execute(mycatConnection, "USE `db1`;");

        execute(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
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
        //execute(mycatConnection, "CREATE TABLE `company` ( `id` int(11) NOT NULL AUTO_INCREMENT,`companyname` varchar(20) DEFAULT NULL,`addressid` int(11) DEFAULT NULL,PRIMARY KEY (`id`))");
        execute(mycatConnection, "delete from db1.travelrecord");

        for (int i = 1; i < 10; i++) {
            execute(mycatConnection, "insert db1.travelrecord (id) values(" + i + ")");
        }

        mycatConnection.close();
    }


}
