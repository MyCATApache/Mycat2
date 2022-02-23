package io.mycat.migrate;

import io.mycat.assemble.ManagerHintTest;
import io.mycat.assemble.MycatTest;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.hint.MigrateHint;
import io.mycat.hint.MigrateListHint;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class MigrateTest implements MycatTest {


    @Test
    public void testEmptyInput() throws Exception {

        try (Connection connection = getMySQLConnection(DB_MYCAT);) {
            execute(connection, RESET_CONFIG);
            execute(connection, "CREATE DATABASE db1");


            execute(connection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");

            execute(connection, "CREATE TABLE db1.`travelrecord_migrate` (\n" +
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

            MigrateHint.Input input = new MigrateHint.Input();
            input.setSchemaName("db1");
            input.setTableName("travelrecord");

            MigrateHint.Output output = new MigrateHint.Output();
            output.setSchemaName("db1");
            output.setTableName("travelrecord_migrate");

            List<Map<String, Object>> maps = executeQuery(connection, MigrateListHint.create().build());
            execute(connection, MigrateHint.create("testEmptyInput", input, output).build());
            TimeUnit.SECONDS.sleep(2);
            List<Map<String, Object>> maps2 = executeQuery(connection, MigrateListHint.create().build());

            String res = maps2.toString();
            Assert.assertTrue(res.contains("NAME=testEmptyInput"));
            Assert.assertTrue(res.contains("COMPLETE=1"));
            System.out.println();

        }
    }


    @Test
    public void testNormalToNormal() throws Exception {

        try (Connection connection = getMySQLConnection(DB_MYCAT);) {
            execute(connection, RESET_CONFIG);
            execute(connection, "CREATE DATABASE db1");


            execute(connection, "CREATE TABLE db1.`input` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");

            deleteData(connection, "db1", "input");

            for (int i = 1; i < 64; i++) {
                execute(connection, "insert db1.input (id) values(" + i + ")");
            }


            execute(connection, "CREATE TABLE db1.`output` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");

            deleteData(connection, "db1", "output");

            MigrateHint.Input input = new MigrateHint.Input();
            input.setSchemaName("db1");
            input.setTableName("input");

            MigrateHint.Output output = new MigrateHint.Output();
            output.setSchemaName("db1");
            output.setTableName("output");
            long right_count = count(connection, "db1", "input");
            List<Map<String, Object>> maps = executeQuery(connection, MigrateListHint.create().build());
            execute(connection, MigrateHint.create("testNormalToNormal", input, output).build());
            TimeUnit.SECONDS.sleep(2);
            long start = System.currentTimeMillis();
            for (; ; ) {
                Thread.sleep(1);
                long count = count(connection, "db1", "output");
                List<Map<String, Object>> maps2 = executeQuery(connection, MigrateListHint.create().build());

                String res = maps2.toString();
                Assert.assertTrue(res.contains("NAME=testNormalToNormal"));
                boolean COMPLETE = res.contains("COMPLETE=1");
                if (COMPLETE&&count==right_count){
                    return;
                }
                if ((TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - start) > 5) {
                    Assert.fail();
                }
            }
        }
    }

    @Test
    public void testNormalToSharding() throws Exception {

        try (Connection connection = getMySQLConnection(DB_MYCAT);) {
            execute(connection, RESET_CONFIG);
            execute(connection, "CREATE DATABASE db1");


            execute(connection, "CREATE TABLE db1.`input` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");

            deleteData(connection, "db1", "input");

            for (int i = 1; i < 64; i++) {
                execute(connection, "insert db1.input (id) values(" + i + ")");
            }


            execute(connection, "CREATE TABLE db1.`output` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8" + " dbpartition by mod_hash(id) tbpartition by mod_hash(id) tbpartitions 2 dbpartitions 2;");

            deleteData(connection, "db1", "output");

            MigrateHint.Input input = new MigrateHint.Input();
            input.setSchemaName("db1");
            input.setTableName("input");

            MigrateHint.Output output = new MigrateHint.Output();
            output.setSchemaName("db1");
            output.setTableName("output");
            long right_count = count(connection, "db1", "input");
            List<Map<String, Object>> maps = executeQuery(connection, MigrateListHint.create().build());
            execute(connection, MigrateHint.create("testNormalToSharding", input, output).build());
            TimeUnit.SECONDS.sleep(2);
            long start = System.currentTimeMillis();
            for (; ; ) {
                Thread.sleep(1);
                long count = count(connection, "db1", "output");
                List<Map<String, Object>> maps2 = executeQuery(connection, MigrateListHint.create().build());

                String res = maps2.toString();
                Assert.assertTrue(res.contains("NAME=testNormalToSharding"));
                boolean COMPLETE = res.contains("COMPLETE=1");
                if (COMPLETE&&count==right_count){
                    return;
                }
                if ((TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - start) > 5) {
                    Assert.fail();
                }
            }
        }
    }

    @Test
    public void testNormalToGlobal() throws Exception {

        try (Connection connection = getMySQLConnection(DB_MYCAT);
             Connection c0 = getMySQLConnection(DB1);
             Connection c1 = getMySQLConnection(DB2);) {
            execute(connection, RESET_CONFIG);
            execute(connection, CreateDataSourceHint
                    .create("ds2",
                            DB2));

            execute(connection, CreateClusterHint.create("c0", Arrays.asList("prototypeDs"), Collections.emptyList()));
            execute(connection, CreateClusterHint.create("c1", Arrays.asList("ds2"), Collections.emptyList()));

            execute(connection, "CREATE DATABASE db1");


            execute(connection, "CREATE TABLE db1.`input` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");

            deleteData(connection, "db1", "input");

            for (int i = 1; i < 64; i++) {
                execute(connection, "insert db1.input (id) values(" + i + ")");
            }


            execute(connection, "CREATE TABLE db1.`output` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8" + " broadcast;");

            deleteData(connection, "db1", "output");

            MigrateHint.Input input = new MigrateHint.Input();
            input.setSchemaName("db1");
            input.setTableName("input");

            MigrateHint.Output output = new MigrateHint.Output();
            output.setSchemaName("db1");
            output.setTableName("output");
            long right_count = count(connection, "db1", "input");
            List<Map<String, Object>> maps = executeQuery(connection, MigrateListHint.create().build());
            execute(connection, MigrateHint.create("testNormalToGlobal", input, output).build());
            TimeUnit.SECONDS.sleep(2);
            long start = System.currentTimeMillis();
            for (; ; ) {
                Thread.sleep(1);
                long mycat_count = count(connection, "db1", "output");
                long c0_count = count(c0, "db1", "output");
                long c1_count = count(c1, "db1", "output");
                List<Map<String, Object>> maps2 = executeQuery(connection, MigrateListHint.create().build());

                String res = maps2.toString();
                Assert.assertTrue(res.contains("NAME=testNormalToGlobal"));
                boolean COMPLETE = res.contains("COMPLETE=1");
                if (COMPLETE&&mycat_count==right_count&&mycat_count==c0_count&&mycat_count==c1_count){
                    return;
                }
                if ((TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - start) > 5) {
                    Assert.fail();
                }
            }
        }
    }


    @Test
    public void testShardingToSharding() throws Exception {

        try (Connection connection = getMySQLConnection(DB_MYCAT);) {
            execute(connection, RESET_CONFIG);
            execute(connection, "CREATE DATABASE db1");


            execute(connection, "CREATE TABLE db1.`input` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8" + " dbpartition by mod_hash(id) tbpartition by mod_hash(id) tbpartitions 2 dbpartitions 2;");

            deleteData(connection, "db1", "input");

            for (int i = 1; i < 64; i++) {
                execute(connection, "insert db1.input (id) values(" + i + ")");
            }


            execute(connection, "CREATE TABLE db1.`output` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8" + " dbpartition by mod_hash(id) tbpartition by mod_hash(id) tbpartitions 2 dbpartitions 2;");

            deleteData(connection, "db1", "output");

            MigrateHint.Input input = new MigrateHint.Input();
            input.setSchemaName("db1");
            input.setTableName("input");

            MigrateHint.Output output = new MigrateHint.Output();
            output.setSchemaName("db1");
            output.setTableName("output");
            long right_count = count(connection, "db1", "input");
            List<Map<String, Object>> maps = executeQuery(connection, MigrateListHint.create().build());
            execute(connection, MigrateHint.create("testShardingToSharding", input, output).build());
            TimeUnit.SECONDS.sleep(2);
            long start = System.currentTimeMillis();
            for (; ; ) {
                Thread.sleep(1);
                long count = count(connection, "db1", "output");
                List<Map<String, Object>> maps2 = executeQuery(connection, MigrateListHint.create().build());

                String res = maps2.toString();
                Assert.assertTrue(res.contains("NAME=testShardingToSharding"));
                boolean COMPLETE = res.contains("COMPLETE=1");
                if (COMPLETE&&count==right_count){
                    return;
                }
                if ((TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - start) > 5) {
                    Assert.fail();
                }
            }
        }
    }
}
