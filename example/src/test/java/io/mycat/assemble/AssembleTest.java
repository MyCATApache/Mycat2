package io.mycat.assemble;

import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class AssembleTest implements MycatTest {

    @Test
    public void testTranscationFail2() throws Exception {
        Consumer<Connection> connectionFunction = new Consumer<Connection>() {

            @SneakyThrows
            @Override
            public void accept(Connection mycatConnection) {
                execute(mycatConnection, "set transaction_policy  = proxy");
            }
        };

        testTranscation(connectionFunction);
    }

    @Test
    public void testTranscationFail() throws Exception {
        Consumer<Connection> connectionFunction = new Consumer<Connection>() {

            @SneakyThrows
            @Override
            public void accept(Connection mycatConnection) {
                AssembleTest.this.execute(mycatConnection, "set transaction_policy  = xa");
            }
        };
        testTranscation(connectionFunction);
    }

    private void testTranscation(Consumer<Connection> connectionFunction) throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);
            initCluster(mycatConnection);
            connectionFunction.accept(mycatConnection);
            execute(mycatConnection, "CREATE DATABASE db1");
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
                    + " dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2 dbpartitions 2;");
            execute(mycatConnection, "use db1");
            deleteData(mycatConnection, "db1", "travelrecord");
            mycatConnection.setAutoCommit(false);
            execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('9999999999');");
            execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('1');");
            execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`,`user_id`) VALUES ('1',999/0);");
        } catch (Throwable ignored) {

        }
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            connectionFunction.accept(mycatConnection);
            execute(mycatConnection, "use db1");
            Assert.assertTrue(executeQuery(mycatConnection,
                    "select *  from travelrecord"
            ).isEmpty());
            mycatConnection.setAutoCommit(false);
            execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('9999999999');");
            execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('1');");
            mycatConnection.commit();
            Assert.assertTrue(hasData(mycatConnection, "db1", "travelrecord"));
        }
    }

    @Test
    public void testBase() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection mysql3306 = getMySQLConnection(DB1);
             Connection mysql3307 = getMySQLConnection(DB2);) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mysql3306, "drop database if exists db1");
            execute(mysql3306, "drop database if exists db1_0");
            execute(mysql3306, "drop database if exists db1_1");

            execute(mysql3307, "drop database if exists db1");
            execute(mysql3307, "drop database if exists db1_0");
            execute(mysql3307, "drop database if exists db1_1");

        }
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {

            List<Map<String, Object>> maps = executeQuery(mycatConnection,
                    "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'db1' UNION SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'xxx' UNION SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = 'db1' ");

            testInfoFunction(mycatConnection);


            execute(mycatConnection, "DROP DATABASE db1");
            Assert.assertFalse(executeQuery(mycatConnection, "show databases").toString().contains("db1"));

            execute(mycatConnection, "CREATE DATABASE db1");
            Assert.assertTrue(executeQuery(mycatConnection, "show databases").toString().contains("db1"));

            execute(mycatConnection, "drop table db1.travelrecord");

            Assert.assertFalse(
                    executeQuery(mycatConnection,
                            "SHOW FULL TABLES FROM `db1` WHERE table_type = 'BASE TABLE';").toString().contains("travelrecord")
            );

            execute(mycatConnection, "USE `db1`;");
            execute(mycatConnection, "CREATE TABLE `travelrecord` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");
            execute(mycatConnection, "/*+ mycat:setSequence{\"name\":\"db1_travelrecord\",\"time\":true} */;");

            Assert.assertTrue(
                    executeQuery(mycatConnection,
                            "SHOW FULL TABLES FROM `db1` WHERE table_type = 'BASE TABLE';").toString().contains("travelrecord")
            );
        }
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection mysql3306 = getMySQLConnection(DB1);
             Connection mysql3307 = getMySQLConnection(DB2);) {

            Assert.assertTrue(
                    executeQuery(mycatConnection, "select * from travelrecord limit 1").isEmpty()
            );

            execute(mycatConnection,
                    "insert  into `travelrecord`(`id`,`user_id`) values (12,'999');"
            );
            List<Map<String, Object>> maps2 = executeQuery(mycatConnection, "select LAST_INSERT_ID()");
            Assert.assertTrue(maps2
                    .toString().contains("12")
            );
            execute(mycatConnection, "\n" +
                    "insert  into `travelrecord`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`) values (1,'999',NULL,NULL,NULL,NULL),(2,NULL,NULL,NULL,NULL,NULL),(6666,NULL,NULL,NULL,NULL,NULL),(999999999,'999',NULL,NULL,NULL,NULL);\n");

            Assert.assertTrue(
                    executeQuery(mycatConnection, "select LAST_INSERT_ID()").toString().contains("999999999")
            );
            Assert.assertEquals("[{ROW_COUNT()=4}]",
                    executeQuery(mycatConnection, "select ROW_COUNT()").toString()
            );
            Assert.assertFalse(
                    executeQuery(mycatConnection, "select * from travelrecord limit 1").isEmpty()
            );
            execute(mycatConnection, "delete from db1.travelrecord");
            Assert.assertFalse(hasData(mycatConnection, "db1", "travelrecord"));
            execute(mycatConnection, "drop table db1.travelrecord");

            execute(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8");


            Assert.assertFalse(existTable(mycatConnection, "db1", "travelreord"));


            ////////////////////////////////////////////end/////////////////////////////////////////

            initCluster(mycatConnection);

            execute(mycatConnection, "delete from db1.travelrecord");
            execute(mycatConnection, "drop table db1.travelrecord");
            execute(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8 BROADCAST;");

            execute(mycatConnection, "delete from db1.travelrecord");
            Assert.assertFalse(hasData(mycatConnection, "db1", "travelrecord"));
            execute(mycatConnection,
                    "insert  into db1.`travelrecord`(`id`,`user_id`) values (12,'999');"
            );

            {
                String sql = "select * from db1.travelrecord";
                String res = executeQuery(mycatConnection, sql).toString();
                Assert.assertEquals(res, executeQuery(mysql3306, sql).toString());
                Assert.assertEquals(res, executeQuery(mysql3307, sql).toString());


            }
            execute(mycatConnection, "drop table db1.travelrecord");

            Assert.assertFalse(existTable(mycatConnection, "db1", "travelreord"));


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
                    + " dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2 dbpartitions 2;");

            Assert.assertTrue(existTable(mycatConnection, "db1", "travelrecord"));
            execute(mycatConnection, "delete from db1.travelrecord");
            Assert.assertFalse(hasData(mycatConnection, "db1", "travelrecord"));
            execute(mycatConnection,
                    "insert  into db1.`travelrecord`(`id`,`user_id`) values (12,'999');"
            );
            Assert.assertTrue(
                    executeQuery(mycatConnection, "select LAST_INSERT_ID()").toString().contains("12")
            );
            execute(mycatConnection, "\n" +
                    "insert  into db1.`travelrecord`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`) values (1,'999',NULL,NULL,NULL,NULL),(2,NULL,NULL,NULL,NULL,NULL),(6666,NULL,NULL,NULL,NULL,NULL),(999999999,'999',NULL,NULL,NULL,NULL);\n");
            Assert.assertTrue(
                    executeQuery(mycatConnection, "select LAST_INSERT_ID()").toString().contains("999999999")
            );
            Assert.assertEquals(executeQuery(mycatConnection, "select * from db1.travelrecord").size(), 5);
            execute(mycatConnection, "delete from db1.travelrecord");
            Assert.assertFalse(hasData(mycatConnection, "db1", "travelrecord"));
            execute(mycatConnection, "\n" +
                    "insert  into db1.`travelrecord`(`user_id`,`traveldate`,`fee`,`days`,`blob`) values ('999',NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),('999',NULL,NULL,NULL,NULL);\n");
            List<Map<String, Object>> maps1 = executeQuery(mycatConnection, "select id from db1.travelrecord");
            execute(mycatConnection, "drop table db1.travelrecord");
            Assert.assertFalse(existTable(mycatConnection, "db1", "travelrecord"));

            testNormalTranscationWrapper(mycatConnection, "set transaction_policy = xa", "xa");
        }
    }

    @Test
    public void testProxyNormalTranscation() throws Exception {
        try (Connection mySQLConnection = getMySQLConnection(DB_MYCAT);) {
            testNormalTranscationWrapper(mySQLConnection, "set transaction_policy = proxy", "proxy");
        }
    }

    @Test
    public void testXANormalTranscation() throws Exception {
        try (Connection mySQLConnection = getMySQLConnection(DB_MYCAT);) {
            testNormalTranscationWrapper(mySQLConnection, "set transaction_policy = xa", "xa");

        }
    }

    private void testNormalTranscationWrapper(Connection mycatConnection, String s, String proxy) throws Exception {
        //////////////////////////////////////transcation/////////////////////////////////////////////

        execute(mycatConnection, s);
        Assert.assertTrue(executeQuery(mycatConnection, "select @@transaction_policy").toString().contains(proxy));

        testProxyNormalTranscation(mycatConnection);
    }

    protected void initCluster(Connection mycatConnection) throws Exception {
        execute(mycatConnection,
                CreateDataSourceHint
                        .create("dw0",DB1));

        execute(mycatConnection,
                CreateDataSourceHint
                        .create("dr0",DB1));

        execute(mycatConnection,
                CreateDataSourceHint
                        .create("dw1",DB2));

        execute(mycatConnection,
                CreateDataSourceHint
                        .create("dr1",DB2));

        execute(mycatConnection,
                CreateClusterHint
                        .create("c0",
                                Arrays.asList("dw0"), Arrays.asList("dr0")));

        execute(mycatConnection,
                CreateClusterHint
                        .create("c1",
                                Arrays.asList("dw1"), Arrays.asList("dr1")));
    }

    @Test
    public void testInfoFunction() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            testInfoFunction(mycatConnection);
        }
    }

    private void testInfoFunction(Connection mycatConnection) throws Exception {
        execute(mycatConnection, RESET_CONFIG);
        // show databases
        executeQuery(mycatConnection, "select database()");


        // use
        execute(mycatConnection, "USE `information_schema`;");
        Assert.assertTrue(executeQuery(mycatConnection, "select database()").toString().contains("information_schema"));
        execute(mycatConnection, "USE `mysql`;");

        // database();
        Assert.assertTrue(executeQuery(mycatConnection, "select database()").toString().contains("mysql"));

        // VERSION()
        executeQuery(mycatConnection, "select VERSION()");

        // LAST_INSERT_ID()
        executeQuery(mycatConnection, "select CONNECTION_ID()");

        // CURRENT_USER()
        executeQuery(mycatConnection, "select CURRENT_USER()");

        // SYSTEM_USER()
        executeQuery(mycatConnection, "select SYSTEM_USER()");

        // SESSION_USER()
        executeQuery(mycatConnection, "select SESSION_USER()");

        executeQuery(mycatConnection, "select SESSION_USER()");
    }

    private void testProxyNormalTranscation(Connection mycatConnection) throws Exception {
        execute(mycatConnection, RESET_CONFIG);
        addC0(mycatConnection);
        execute(mycatConnection, "CREATE DATABASE db1");
        execute(mycatConnection, "use db1");
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
                + " dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2 dbpartitions 2;");

        deleteData(mycatConnection, "db1", "travelrecord");
        mycatConnection.setAutoCommit(false);
        Assert.assertTrue(
                executeQuery(mycatConnection, "SELECT @@autocommit;").toString().contains("0")
        );
        execute(mycatConnection,
                "insert  into `travelrecord`(`id`,`user_id`) values (1,'999'),(999999999,'999');");
        mycatConnection.rollback();

        mycatConnection.setAutoCommit(true);
        Assert.assertTrue(
                executeQuery(mycatConnection, "SELECT @@autocommit;").toString().contains("1")
        );
        Assert.assertFalse(hasData(mycatConnection, "db1", "travelrecord"));


        ///////////////////////////////////////////////////////////////////////////////////////
        mycatConnection.setAutoCommit(false);
        Assert.assertTrue(
                executeQuery(mycatConnection, "SELECT @@autocommit;").toString().contains("0")
        );
        execute(mycatConnection,
                "insert  into `travelrecord`(`id`,`user_id`) values (1,'999'),(999999999,'999');");
        mycatConnection.commit();

        mycatConnection.setAutoCommit(true);
        Assert.assertTrue(
                executeQuery(mycatConnection, "SELECT @@autocommit;").toString().contains("1")
        );
        Assert.assertEquals(2, executeQuery(mycatConnection, "select id from db1.travelrecord").size());
    }

}
