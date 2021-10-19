package io.mycat.gsi;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.hint.ShowTopologyHint;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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
    boolean init = false;

    @Before
    public void before() throws Exception {
        if (!init) {
            try (Connection connection = getMySQLConnection(DB_MYCAT)) {
                JdbcUtils.execute(connection, "/*+ mycat:readXARecoveryLog{} */;");
            }
            init = true;
        }
    }

    @Test
    public void createGsi() throws Exception {
        initShardingTable();
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection db1 = getMySQLConnection(DB1);
             Connection db2 = getMySQLConnection(DB2)) {
//            execute(db1,"DROP TABLE if exists db1_0.travelrecord_g_i_user_id_0");
//            execute(db1,"DROP TABLE if exists db1_0.travelrecord_g_i_user_id_1");
//            execute(db2,"DROP TABLE if exists db1_1.travelrecord_g_i_user_id_2");
//            execute(db2,"DROP TABLE if exists db1_1.travelrecord_g_i_user_id_3");

            execute(mycatConnection, "CREATE UNIQUE GLOBAL INDEX `g_i_user_id` ON `db1`.`travelrecord`(`user_id`) \n" +
                    "    COVERING(`fee`,id) \n" +
                    "    dbpartition by mod_hash(`user_id`) tbpartition by mod_hash(`user_id`)  tbpartitions 2");
            boolean b = hasData(mycatConnection, "db1", "travelrecord_g_i_user_id");//test create it
            List<Map<String, Object>> travelrecord_g_i_user_id_topologyHint = executeQuery(mycatConnection, ShowTopologyHint.create("db1", "travelrecord_g_i_user_id").toString());
            Assert.assertEquals("[{targetName=c0, schemaName=db1_0, tableName=travelrecord_g_i_user_id_0, dbIndex=0, tableIndex=0, index=0}," +
                    " {targetName=c0, schemaName=db1_0, tableName=travelrecord_g_i_user_id_1, dbIndex=0, tableIndex=1, index=1}, " +
                    "{targetName=c1, schemaName=db1_1, tableName=travelrecord_g_i_user_id_2, dbIndex=1, tableIndex=0, index=2}, " +
                    "{targetName=c1, schemaName=db1_1, tableName=travelrecord_g_i_user_id_3, dbIndex=1, tableIndex=1, index=3}]", travelrecord_g_i_user_id_topologyHint.toString());


            String explainPrimaryTable = explain(mycatConnection, "select * from db1.travelrecord where id = 1");
            Assert.assertTrue(explainPrimaryTable.contains("Each(targetName=c0, sql=SELECT * FROM db1_0.travelrecord_1 AS `travelrecord` WHERE (`travelrecord`.`id` = ?))"));
            String explainIndexScan = explain(mycatConnection, "select * from db1.travelrecord where user_id = 1");//index-scan
            Assert.assertTrue(explainIndexScan.contains("MycatSQLTableLookup"));
            String explainOnlyIndexScan = explain(mycatConnection, "select fee from db1.travelrecord where user_id = 1");//index-scan
            Assert.assertTrue(explainOnlyIndexScan.contains("Each(targetName=c0, sql=SELECT `travelrecord_g_i_user_id`.`fee` FROM db1_0.travelrecord_g_i_user_id_1 AS `travelrecord_g_i_user_id` WHERE (`travelrecord_g_i_user_id`.`user_id` = ?))"));

            String explain;
            explain = explain(mycatConnection, "delete  from db1.travelrecord");
            Assert.assertTrue(explain.contains("travelrecord_g_i_user_id"));
            deleteData(mycatConnection, "db1", "travelrecord");
            deleteData(mycatConnection, "db1", "travelrecord_g_i_user_id");

            explain = explain(mycatConnection, "insert db1.travelrecord (id,user_id) values(1,2)");
            Assert.assertTrue(explain.contains("travelrecord_g_i_user_id"));
            for (int i = 0; i < 10; i++) {
                execute(mycatConnection, "insert db1.travelrecord (id,user_id) values(" + i + "," +
                        "" +
                        i +
                        ")");
            }
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "select fee from db1.travelrecord where user_id = 1");
            Assert.assertEquals(1, maps.size());
            System.out.println();

            //测试事务
            long count0 = count(mycatConnection, "db1", "travelrecord");
            long count1 = count(mycatConnection, "db1", "travelrecord_g_i_user_id");

            Assert.assertEquals(count0, count1);
            String explain1 = explain(mycatConnection, "insert db1.travelrecord (id,user_id) values(" + 100 + "," +
                    "" +
                    100 +
                    ")");
            Assert.assertTrue(explain1.contains("travelrecord_0") && explain1.contains("travelrecord_g_i_user_id_1"));

            mycatConnection.setAutoCommit(false);
            for (int i = 10; i < 20; i++) {
                execute(mycatConnection, "insert db1.travelrecord (id,user_id) values(" + i + "," +
                        "" +
                        i +
                        ")");
            }
            mycatConnection.rollback();

            long _count0 = count(mycatConnection, "db1", "travelrecord");
            long _count1 = count(mycatConnection, "db1", "travelrecord_g_i_user_id");
            Assert.assertEquals(count0, _count0);
            Assert.assertEquals(count1, _count1);

            for (int i = 10; i < 20; i++) {
                execute(mycatConnection, "insert db1.travelrecord (id,user_id) values(" + i + "," +
                        "" +
                        i +
                        ")");
            }
            mycatConnection.setAutoCommit(true);
            _count0 = count(mycatConnection, "db1", "travelrecord");
            _count1 = count(mycatConnection, "db1", "travelrecord_g_i_user_id");
            Assert.assertEquals(20, _count0);
            Assert.assertEquals(20, _count1);

            List<Map<String, Object>> maps2 = executeQuery(mycatConnection, "select count(1) from db1.travelrecord where user_id = 1");
            List<Map<String, Object>> maps3 = executeQuery(mycatConnection, "select t.id from db1.travelrecord t  LEFT JOIN db1.company c on t.user_id  = c.id limit 10");
            Assert.assertEquals(1, maps2.size());
            Assert.assertEquals(10, maps3.size());

            List<Map<String, Object>> maps4 = executeQuery(mycatConnection, "select t.id from db1.travelrecord t  LEFT JOIN db1.company c on t.user_id  = c.id where t.user_id = 1 limit 10");
            Assert.assertEquals(1, maps4.size());

            List<Map<String, Object>> maps5 = executeQuery(mycatConnection, "select t.id from db1.travelrecord t  LEFT JOIN db1.company2 c on t.user_id  = c.id  where t.user_id = 1  limit 10");
            Assert.assertEquals(1, maps5.size());

            testInsertException(mycatConnection, TranscationType.XA);
            testInsertException(mycatConnection, TranscationType.PROXY);

            //测试索引注解
            String sql = "SELECT * FROM db1.travelrecord FORCE INDEX(g_i_user_id) WHERE user_id =1 ";
            String e = explain(mycatConnection, sql);
            Assert.assertTrue(e.contains("travelrecord_g_i_user_id"));
            List<Map<String, Object>> maps1 = executeQuery(mycatConnection, sql);
            Assert.assertEquals("[{id=1, user_id=1, traveldate=null, fee=null, days=null, blob=null}]", maps1.toString());
            Assert.assertEquals("[{id=1, user_id=1, traveldate=null, fee=null, days=null, blob=null}]",
                    executeQuery(mycatConnection, "SELECT * FROM db1.travelrecord WHERE user_id =1 ").toString());
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
        switch (transcationType) {
            case XA:
                Assert.assertEquals(_count0, count(connection, "db1", "travelrecord"));
                Assert.assertEquals(_count1, count(connection, "db1", "travelrecord_g_i_user_id"));
                break;
            case PROXY:
                Assert.assertEquals(count(connection, "db1", "travelrecord"), count(connection, "db1", "travelrecord_g_i_user_id"));
                break;
        }

    }


    @Test
    public void createGsi2() throws Exception {
        initShardingTable();
        try (Connection connection = getMySQLConnection(DB_MYCAT)) {
            execute(connection, "CREATE TABLE IF NOT EXISTS db1.`travelrecord` (\n\t`id` bigint NOT NULL AUTO_INCREMENT,\n\t`user_id` varchar(100) DEFAULT NULL,\n\t`traveldate` date DEFAULT NULL,\n\t`fee` decimal(10, 0) DEFAULT NULL,\n\t`days` int DEFAULT NULL,\n\t`blob` longblob,\n\tPRIMARY KEY (`id`),\n\tKEY `id` (`id`),\n\tGLOBAL INDEX `g_i_user_id`(`user_id`) COVERING (`fee`, id) DBPARTITION BY mod_hash(`user_id`) TBPARTITION BY mod_hash(`user_id`)  TBPARTITIONS 2\n) ENGINE = InnoDB CHARSET = utf8\nDBPARTITION BY mod_hash(id) DBPARTITIONS 2\nTBPARTITION BY mod_hash(id) TBPARTITIONS 2");
            boolean b = hasData(connection, "db1", "travelrecord_g_i_user_id");//test create it
            List<Map<String, Object>> travelrecord_g_i_user_id_topologyHint = executeQuery(connection, ShowTopologyHint.create("db1", "travelrecord_g_i_user_id").toString());
            Assert.assertEquals("[{targetName=c0, schemaName=db1_0, tableName=travelrecord_g_i_user_id_0, dbIndex=0, tableIndex=0, index=0}, {targetName=c0, schemaName=db1_0, tableName=travelrecord_g_i_user_id_1, dbIndex=0, tableIndex=1, index=1}, {targetName=c1, schemaName=db1_1, tableName=travelrecord_g_i_user_id_2, dbIndex=1, tableIndex=0, index=2}, {targetName=c1, schemaName=db1_1, tableName=travelrecord_g_i_user_id_3, dbIndex=1, tableIndex=1, index=3}]", travelrecord_g_i_user_id_topologyHint.toString());


            String explainPrimaryTable = explain(connection, "select * from db1.travelrecord where id = 1");
            Assert.assertTrue(explainPrimaryTable.contains("Each(targetName=c0, sql=SELECT * FROM db1_0.travelrecord_1 AS `travelrecord` WHERE (`travelrecord`.`id` = ?))"));
            String explainIndexScan = explain(connection, "select * from db1.travelrecord where user_id = 1");//index-scan
            Assert.assertTrue(explainIndexScan.contains("MycatSQLTableLookup"));
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


    @Test
    public void createGsi3() throws Exception {
        Connection mycatConnection = getMySQLConnection(DB_MYCAT);
        execute(mycatConnection, RESET_CONFIG);

        execute(mycatConnection, "DROP DATABASE db2");


        execute(mycatConnection, "CREATE DATABASE db2");

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

        execute(mycatConnection, "USE `db2`;");

        execute(mycatConnection,"CREATE TABLE db2.normal (\n\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n\t`user_id` varchar(100) DEFAULT NULL,\n\t`traveldate` date DEFAULT NULL,\n\t`fee` decimal(10, 0) DEFAULT NULL,\n\t`days` int(11) DEFAULT NULL,\n\t`blob` longblob,\n\tPRIMARY KEY (`id`),\n\tKEY `id` (`id`)\n) ENGINE = InnoDB AUTO_INCREMENT = 1129569 CHARSET = utf8 ");
        execute(mycatConnection,"CREATE TABLE IF NOT EXISTS db2.`fp` (\n\t`id` bigint NOT NULL AUTO_INCREMENT,\n\t`user_id` varchar(100) DEFAULT NULL,\n\t`traveldate` date DEFAULT NULL,\n\t`fee` decimal(10, 0) DEFAULT NULL,\n\t`days` int DEFAULT NULL,\n\t`blob` longblob,\n\tPRIMARY KEY (`id`),\n\tKEY `id` (`id`),\n\tGLOBAL INDEX `g_i_user_id`(`user_id`) COVERING (`fee`, id) DBPARTITION BY mod_hash(`user_id`) TBPARTITION BY mod_hash(`user_id`) TBPARTITIONS 2\n) ENGINE = InnoDB CHARSET = utf8\nDBPARTITION BY mod_hash(id) DBPARTITIONS 2\nTBPARTITION BY mod_hash(id) TBPARTITIONS 2");
       deleteData(mycatConnection,"db2","fp");
        execute(mycatConnection,"INSERT INTO `db2`.`fp` (`id`, `user_id`) VALUES ('1', '1'); ");
        List<Map<String, Object>> maps = executeQuery(mycatConnection, "\n" +
                " SELECT a.id\n" +
                "FROM db2.fp a\n" +
                "\tLEFT JOIN db2.normal b ON a.user_id = b.user_id\n" +
                "WHERE a.user_id = 1\n" +
                "LIMIT 10\n");
        Assert.assertEquals(1,maps.size());
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

        execute(mycatConnection, "CREATE TABLE if not exists db1.`company` ( `id` int(11) NOT NULL AUTO_INCREMENT,`companyname` varchar(20) DEFAULT NULL,`addressid` int(11) DEFAULT NULL,PRIMARY KEY (`id`)) broadcast");
        execute(mycatConnection, "CREATE TABLE if not exists db1.`company2` ( `id` int(11) NOT NULL AUTO_INCREMENT,`companyname` varchar(20) DEFAULT NULL,`addressid` int(11) DEFAULT NULL,PRIMARY KEY (`id`))");


        mycatConnection.close();
    }


}
