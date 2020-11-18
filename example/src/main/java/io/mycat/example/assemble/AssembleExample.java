package io.mycat.example.assemble;

import com.alibaba.druid.util.JdbcUtils;
import com.mysql.cj.jdbc.MysqlDataSource;
import io.mycat.config.ClusterConfig;
import io.mycat.config.DatasourceConfig;
import io.mycat.example.TestUtil;
import io.mycat.hint.AddClusterHint;
import io.mycat.hint.AddDatasourceHint;
import io.mycat.util.JsonUtil;
import lombok.SneakyThrows;
import org.apache.calcite.linq4j.function.Function;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class AssembleExample {



    Connection getMySQLConnection(int port) throws SQLException {
        String username = "root";
        String password = "123456";
//        properties.put("useBatchMultiSend", "false");
//        properties.put("usePipelineAuth", "false");
        String url = "jdbc:mysql://127.0.0.1:" +
                port +
                "/?useServerPrepStmts=false&useCursorFetch=false&serverTimezone=UTC&allowMultiQueries=false&useBatchMultiSend=false&characterEncoding=utf8";
        MysqlDataSource mysqlDataSource = new MysqlDataSource();
        mysqlDataSource.setUrl(url);
        mysqlDataSource.setUser(username);
        mysqlDataSource.setPassword(password);
     
        return mysqlDataSource.getConnection();
    }
    @Test
    public void testTranscationFail2() throws Exception {
        Consumer<Connection> connectionFunction = new Consumer<Connection>() {

            @SneakyThrows
            @Override
            public void accept(Connection mycatConnection) {
                AssembleExample.this.execute(mycatConnection, "set xa  = off");
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
                AssembleExample.this.execute(mycatConnection, "set xa  = on");
            }
        };
        testTranscation(connectionFunction);
    }

    private void testTranscation(Consumer<Connection> connectionFunction) throws SQLException {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
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
        try (Connection mycatConnection = getMySQLConnection(8066)) {
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
        Connection mycatConnection = getMySQLConnection(8066);

        Connection mysql3306 = getMySQLConnection(3306);
        Connection mysql3307 = getMySQLConnection(3307);

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

        Assert.assertTrue(
                executeQuery(mycatConnection,
                        "SHOW FULL TABLES FROM `db1` WHERE table_type = 'BASE TABLE';").toString().contains("travelrecord")
        );

        Assert.assertTrue(
                executeQuery(mycatConnection, "select * from travelrecord limit 1").isEmpty()
        );

        execute(mycatConnection,
                "insert  into `travelrecord`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`) values (12,'999',NULL,NULL,NULL,NULL);"
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

        Assert.assertFalse(
                executeQuery(mycatConnection, "select * from travelrecord limit 1").isEmpty()
        );

        execute(mycatConnection, "drop table db1.travelrecord");

        execute(mycatConnection, "CREATE TABLE `travelrecord` (\n" +
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
        execute(mycatConnection,
                "insert  into db1.`travelrecord`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`) values (12,'999',NULL,NULL,NULL,NULL);"
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
                + " dbpartition by hash(id) tbpartition by hash(user_id) tbpartitions 2 dbpartitions 2;");

        Assert.assertTrue(existTable(mycatConnection, "db1", "travelrecord"));
        execute(mycatConnection, "delete from db1.travelrecord");
        execute(mycatConnection,
                "insert  into db1.`travelrecord`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`) values (12,'999',NULL,NULL,NULL,NULL);"
        );
        Assert.assertTrue(
                executeQuery(mycatConnection, "select LAST_INSERT_ID()").toString().contains("12")
        );
        execute(mycatConnection, "\n" +
                "insert  into `travelrecord`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`) values (1,'999',NULL,NULL,NULL,NULL),(2,NULL,NULL,NULL,NULL,NULL),(6666,NULL,NULL,NULL,NULL,NULL),(999999999,'999',NULL,NULL,NULL,NULL);\n");
        Assert.assertTrue(
                executeQuery(mycatConnection, "select LAST_INSERT_ID()").toString().contains("999999999")
        );
        Assert.assertEquals(executeQuery(mycatConnection, "select * from db1.travelrecord").size(), 5);
        execute(mycatConnection, "delete from db1.travelrecord");
        execute(mycatConnection, "\n" +
                "insert  into `travelrecord`(`user_id`,`traveldate`,`fee`,`days`,`blob`) values ('999',NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),('999',NULL,NULL,NULL,NULL);\n");
        List<Map<String, Object>> maps1 = executeQuery(mycatConnection, "select id from db1.travelrecord");
        execute(mycatConnection, "drop table db1.travelrecord");
        Assert.assertFalse(existTable(mycatConnection, "db1", "travelrecord"));
        //////////////////////////////////////transcation/////////////////////////////////////////////

        execute(mycatConnection, "set xa = 0");
        Assert.assertTrue(executeQuery(mycatConnection, "select @@xa").toString().contains("0"));

        testNormalTranscation(mycatConnection);

        execute(mycatConnection, "set xa = 1");
        Assert.assertTrue(executeQuery(mycatConnection, "select @@xa").toString().contains("1"));

        testNormalTranscation(mycatConnection);
    }

    protected void initCluster(Connection mycatConnection) throws SQLException {
        execute(mycatConnection,
                AddDatasourceHint
                        .create("dw0",
                                "jdbc:mysql://127.0.0.1:3306"));

        execute(mycatConnection,
                AddDatasourceHint
                        .create("dr0",
                                "jdbc:mysql://127.0.0.1:3306"));

        execute(mycatConnection,
                AddDatasourceHint
                        .create("dw1",
                                "jdbc:mysql://127.0.0.1:3307"));

        execute(mycatConnection,
                AddDatasourceHint
                        .create("dr1",
                                "jdbc:mysql://127.0.0.1:3307"));

        execute(mycatConnection,
                AddClusterHint
                        .create("c0",
                                Arrays.asList("dw0"), Arrays.asList("dr0")));

        execute(mycatConnection,
                AddClusterHint
                        .create("c1",
                                Arrays.asList("dw1"), Arrays.asList("dr1")));
    }

    @Test
    public void testInfoFunction() throws SQLException {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            testInfoFunction(mycatConnection);
        }
    }

    private void testInfoFunction(Connection mycatConnection) throws SQLException {
        // show databases
        executeQuery(mycatConnection, "select database()");


        // use
        execute(mycatConnection, "USE `information_schema`;");
        Assert.assertTrue(executeQuery(mycatConnection, "select database()").toString().contains("information_schema"));
        execute(mycatConnection, "USE `mysql`;");

        // database();
        Assert.assertTrue(executeQuery(mycatConnection, "select database()").toString().contains("mysql"));

        // VERSION()
        Assert.assertTrue(executeQuery(mycatConnection, "select VERSION()").toString().contains("8.19"));

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

    private void testNormalTranscation(Connection mycatConnection) throws SQLException {
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
                + " dbpartition by hash(id) tbpartition by hash(user_id) tbpartitions 2 dbpartitions 2;");

        deleteData(mycatConnection, "db1", "travelrecord");
        mycatConnection.setAutoCommit(false);
        Assert.assertTrue(
                executeQuery(mycatConnection, "SELECT @@autocommit;").toString().contains("0")
        );
        execute(mycatConnection,
                "insert  into `travelrecord`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`) values (1,'999',NULL,NULL,NULL,NULL),(999999999,'999',NULL,NULL,NULL,NULL);");
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
                "insert  into `travelrecord`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`) values (1,'999',NULL,NULL,NULL,NULL),(999999999,'999',NULL,NULL,NULL,NULL);");
        mycatConnection.commit();

        mycatConnection.setAutoCommit(true);
        Assert.assertTrue(
                executeQuery(mycatConnection, "SELECT @@autocommit;").toString().contains("1")
        );
        Assert.assertEquals(2, executeQuery(mycatConnection, "select id from db1.travelrecord").size());
    }

    private boolean existTable(Connection connection, String db, String table) throws SQLException {
        return !executeQuery(connection, String.format("SHOW TABLES from %s LIKE '%s';", db, table)).isEmpty();

    }

    private boolean hasData(Connection connection, String db, String table) throws SQLException {
        return !executeQuery(connection, String.format("select * from %s.%s limit 1", db, table)).isEmpty();
    }

    private void deleteData(Connection connection, String db, String table) throws SQLException {
        execute(connection, String.format("delete  from %s.%s", db, table));
    }

    protected void execute(Connection mySQLConnection, String sql) throws SQLException {
         JdbcUtils.execute(mySQLConnection, sql);
    }

    public static List<Map<String, Object>> executeQuery(Connection mySQLConnection, String sql) throws SQLException {
        return JdbcUtils.executeQuery(mySQLConnection, sql, Collections.emptyList());
    }

}
