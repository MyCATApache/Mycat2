package io.mycat.example.assemble;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.example.ExampleObject;
import io.mycat.example.TestUtil;
import io.mycat.example.sharding.ShardingExample;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AssembleExample {

    @Test
    public void testWrapper() throws Exception {
        Connection mySQLConnection = TestUtil.getMySQLConnection(8066);

        List<Map<String, Object>> maps = executeQuery(mySQLConnection,
                "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'db1' UNION SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'xxx' UNION SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = 'db1' ");

        // show databases
        executeQuery(mySQLConnection, "show databases");


        // use
        execute(mySQLConnection, "USE `information_schema`;");
        Assert.assertTrue(executeQuery(mySQLConnection, "select database()").toString().contains("information_schema"));
        execute(mySQLConnection, "USE `mysql`;");

        // database();
        Assert.assertTrue(executeQuery(mySQLConnection, "select database()").toString().contains("mysql"));

        // VERSION()
        Assert.assertTrue(executeQuery(mySQLConnection, "select VERSION()").toString().contains("8.19"));

        // LAST_INSERT_ID()
        executeQuery(mySQLConnection, "select CONNECTION_ID()");

        // CURRENT_USER()
        executeQuery(mySQLConnection, "select CURRENT_USER()");

        // SYSTEM_USER()
        executeQuery(mySQLConnection, "select SYSTEM_USER()");

        // SESSION_USER()
        executeQuery(mySQLConnection, "select SESSION_USER()");

        executeQuery(mySQLConnection, "select SESSION_USER()");


        execute(mySQLConnection, "DROP DATABASE db1");
        Assert.assertFalse(executeQuery(mySQLConnection, "show databases").toString().contains("db1"));

        execute(mySQLConnection, "CREATE DATABASE db1");
        Assert.assertTrue(executeQuery(mySQLConnection, "show databases").toString().contains("db1"));

        execute(mySQLConnection, "drop table db1.travelrecord");

        Assert.assertFalse(
                executeQuery(mySQLConnection,
                        "SHOW FULL TABLES FROM `db1` WHERE table_type = 'BASE TABLE';").toString().contains("travelrecord")
        );

        execute(mySQLConnection, "USE `db1`;");
        execute(mySQLConnection, "CREATE TABLE `travelrecord` (\n" +
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
                executeQuery(mySQLConnection,
                        "SHOW FULL TABLES FROM `db1` WHERE table_type = 'BASE TABLE';").toString().contains("travelrecord")
        );

        Assert.assertTrue(
                executeQuery(mySQLConnection, "select * from travelrecord limit 1").isEmpty()
        );

        execute(mySQLConnection,
                "insert  into `travelrecord`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`) values (12,'999',NULL,NULL,NULL,NULL);"
        );

        Assert.assertTrue(
                executeQuery(mySQLConnection, "select LAST_INSERT_ID()").toString().contains("12")
        );
        execute(mySQLConnection, "\n" +
                "insert  into `travelrecord`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`) values (1,'999',NULL,NULL,NULL,NULL),(2,NULL,NULL,NULL,NULL,NULL),(6666,NULL,NULL,NULL,NULL,NULL),(999999999,'999',NULL,NULL,NULL,NULL);\n");

        Assert.assertTrue(
                executeQuery(mySQLConnection, "select LAST_INSERT_ID()").toString().contains("999999999")
        );

        Assert.assertFalse(
                executeQuery(mySQLConnection, "select * from travelrecord limit 1").isEmpty()
        );

        execute(mySQLConnection, "drop table db1.travelrecord");

        execute(mySQLConnection, "CREATE TABLE `travelrecord` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `user_id` varchar(100) DEFAULT NULL,\n" +
                "  `traveldate` date DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int DEFAULT NULL,\n" +
                "  `blob` longblob,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `id` (`id`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf COLLATE=utf8");


        Assert.assertTrue(
                executeQuery(mySQLConnection, "select * from travelrecord limit 1").isEmpty()
        );


        execute(mySQLConnection, "drop table db1.travelrecord");


        ////////////////////////////////////////////end/////////////////////////////////////////

        System.out.println();
    }

    private void execute(Connection mySQLConnection, String sql) throws SQLException {
        JdbcUtils.execute(mySQLConnection, sql);
    }


    public static List<Map<String, Object>> executeQuery(Connection mySQLConnection, String sql) throws SQLException {
        return JdbcUtils.executeQuery(mySQLConnection, sql, Collections.emptyList());
    }

}
