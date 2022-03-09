package io.mycat.binlog;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import io.mycat.hint.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
@Ignore
public class BinlogTest implements MycatTest {


    @Before
    public void init() throws Exception {
        try (Connection mySQLConnection = getMySQLConnection(DB1);) {
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mySQLConnection, "SHOW VARIABLES LIKE 'log_bin'", Collections.emptyList());
            Assert.assertTrue(maps.toString().contains("ON"));
        }
    }
    @Test
    @Ignore
    public void testBinlogSnapshot() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, BinlogClearHint.create());
            String name = "testBinlogSnapshot";
            List<Map<String, Object>> info = executeQuery(mycatConnection, BinlogSnapshotHint.create(name));
            Assert.assertFalse(info.isEmpty());
            System.out.println();
        }
    }
    @Test
    @Ignore
    public void testBinlog() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, BinlogClearHint.create());
            String name = "test0";
            execute(mycatConnection, "CREATE DATABASE db1");


            execute(mycatConnection, "CREATE TABLE db1.`travelrecord_input` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");

            execute(mycatConnection, "CREATE TABLE db1.`travelrecord_output` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");

            List<String> inputTableNames = Arrays.asList("db1.travelrecord_input");
            List<String> outputTableNames = Arrays.asList("db1.travelrecord_output");
            List<Map<String, Object>> info = executeQuery(mycatConnection, BinlogSyncHint.create(name, inputTableNames, outputTableNames));
            Assert.assertTrue(info.toString().contains(name));
            info = executeQuery(mycatConnection, BinlogListHint.create());
            Assert.assertTrue(info.toString().contains(name));
            String id = (String) info.get(0).get("ID").toString();
            execute(mycatConnection, BinlogStopHint.create(id));
            info = executeQuery(mycatConnection, BinlogListHint.create());
            Assert.assertTrue(!info.toString().contains(name));
            System.out.println();
        }
    }

    @Test
    @Ignore
    public void testBinlogNormalToNormal() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, BinlogClearHint.create());
            execute(mycatConnection, RESET_CONFIG);
            String name = "testBinlogNormalToNormal";
            execute(mycatConnection, "CREATE DATABASE db1");


            execute(mycatConnection, "CREATE TABLE db1.`travelrecord_input` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");

            execute(mycatConnection, "CREATE TABLE db1.`travelrecord_output` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");

            List<String> inputTableNames = Arrays.asList("db1.travelrecord_input");
            List<String> outputTableNames = Arrays.asList("db1.travelrecord_output");
            List<Map<String, Object>> info = executeQuery(mycatConnection, BinlogSyncHint.create(name, inputTableNames, outputTableNames));


            deleteData(mycatConnection, "db1", "travelrecord_input");
            deleteData(mycatConnection, "db1", "travelrecord_output");

            execute(mycatConnection, "insert  into db1.`travelrecord_input`(`id`,`traveldate`,`fee`,`days`,`blob`) values ('1',NULL,NULL,NULL,NULL),('2',NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),('3',NULL,NULL,NULL,NULL);\n");
            execute(mycatConnection, "UPDATE `db1`.`travelrecord_input` SET `fee` = '2' WHERE `id` = '3'; ");
            execute(mycatConnection, "delete from db1.travelrecord_input");

            check(mycatConnection);

            execute(mycatConnection, "insert  into db1.`travelrecord_input`(`id`,`traveldate`,`fee`,`days`,`blob`) values ('4',NULL,NULL,NULL,NULL),('5',NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),('6',NULL,NULL,NULL,NULL);\n");
            execute(mycatConnection, "UPDATE `db1`.`travelrecord_input` SET `fee` = '2' WHERE `id` = '4'; ");

            mycatConnection.setAutoCommit(false);
            execute(mycatConnection, "insert  into db1.`travelrecord_input`(`id`,`traveldate`,`fee`,`days`,`blob`) values ('7',NULL,NULL,NULL,NULL),('8',NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),('9',NULL,NULL,NULL,NULL);\n");
            execute(mycatConnection, "UPDATE `db1`.`travelrecord_input` SET `fee` = '2' WHERE `id` = '5'; ");
            mycatConnection.setAutoCommit(true);

            check(mycatConnection);

            System.out.println();
        }
    }


    @Test
    @Ignore
    public void testBinlogGlobalToNormal() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, BinlogClearHint.create());
            execute(mycatConnection, RESET_CONFIG);
            String name = "testBinlogGlobalToNormal";
            execute(mycatConnection, "CREATE DATABASE db1");


            execute(mycatConnection, "CREATE TABLE db1.`travelrecord_input` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8 broadcast");

            execute(mycatConnection, "CREATE TABLE db1.`travelrecord_output` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");

            List<String> inputTableNames = Arrays.asList("db1.travelrecord_input");
            List<String> outputTableNames = Arrays.asList("db1.travelrecord_output");
            List<Map<String, Object>> info = executeQuery(mycatConnection, BinlogSyncHint.create(name, inputTableNames, outputTableNames));


            deleteData(mycatConnection, "db1", "travelrecord_input");
            deleteData(mycatConnection, "db1", "travelrecord_output");

            execute(mycatConnection, "insert  into db1.`travelrecord_input`(`id`,`traveldate`,`fee`,`days`,`blob`) values ('1',NULL,NULL,NULL,NULL),('2',NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),('3',NULL,NULL,NULL,NULL);\n");
            execute(mycatConnection, "UPDATE `db1`.`travelrecord_input` SET `fee` = '2' WHERE `id` = '3'; ");
            execute(mycatConnection, "delete from db1.travelrecord_input");

            check(mycatConnection);

            execute(mycatConnection, "insert  into db1.`travelrecord_input`(`id`,`traveldate`,`fee`,`days`,`blob`) values ('4',NULL,NULL,NULL,NULL),('5',NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),('6',NULL,NULL,NULL,NULL);\n");
            execute(mycatConnection, "UPDATE `db1`.`travelrecord_input` SET `fee` = '2' WHERE `id` = '4'; ");

            check(mycatConnection);

            mycatConnection.setAutoCommit(false);
            execute(mycatConnection, "insert  into db1.`travelrecord_input`(`id`,`traveldate`,`fee`,`days`,`blob`) values ('7',NULL,NULL,NULL,NULL),('8',NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),('9',NULL,NULL,NULL,NULL);\n");
            execute(mycatConnection, "UPDATE `db1`.`travelrecord_input` SET `fee` = '2' WHERE `id` = '5'; ");
            mycatConnection.setAutoCommit(true);

            check(mycatConnection);

            System.out.println();
        }
    }

    @Test
    @Ignore
    public void testBinlogShardingToNormal() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, BinlogClearHint.create());
            execute(mycatConnection, RESET_CONFIG);
            String name = "testBinlogGlobalToNormal";
            execute(mycatConnection, "CREATE DATABASE db1");


            execute(mycatConnection, "CREATE TABLE db1.`travelrecord_input` (\n" +
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

            execute(mycatConnection, "CREATE TABLE db1.`travelrecord_output` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");

            List<String> inputTableNames = Arrays.asList("db1.travelrecord_input");
            List<String> outputTableNames = Arrays.asList("db1.travelrecord_output");
            List<Map<String, Object>> info = executeQuery(mycatConnection, BinlogSyncHint.create(name, inputTableNames, outputTableNames));


            deleteData(mycatConnection, "db1", "travelrecord_input");
            deleteData(mycatConnection, "db1", "travelrecord_output");

            check(mycatConnection);

            execute(mycatConnection, "insert  into db1.`travelrecord_input`(`id`,`traveldate`,`fee`,`days`,`blob`) values ('1',NULL,NULL,NULL,NULL),('2',NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),('3',NULL,NULL,NULL,NULL);\n");
            execute(mycatConnection, "UPDATE `db1`.`travelrecord_input` SET `fee` = '2' WHERE `id` = '3'; ");
            execute(mycatConnection, "delete from db1.travelrecord_input");

            execute(mycatConnection, "insert  into db1.`travelrecord_input`(`id`,`traveldate`,`fee`,`days`,`blob`) values ('4',NULL,NULL,NULL,NULL),('5',NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),('6',NULL,NULL,NULL,NULL);\n");
            execute(mycatConnection, "UPDATE `db1`.`travelrecord_input` SET `fee` = '2' WHERE `id` = '4'; ");

            mycatConnection.setAutoCommit(false);
            execute(mycatConnection, "insert  into db1.`travelrecord_input`(`id`,`traveldate`,`fee`,`days`,`blob`) values ('7',NULL,NULL,NULL,NULL),('8',NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),('9',NULL,NULL,NULL,NULL);\n");
            execute(mycatConnection, "UPDATE `db1`.`travelrecord_input` SET `fee` = '2' WHERE `id` = '5'; ");
            mycatConnection.setAutoCommit(true);

            check(mycatConnection);

            System.out.println();
        }
    }


    @Test
    @Ignore
    public void testBinlogShardingToSharding() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, BinlogClearHint.create());
            execute(mycatConnection, RESET_CONFIG);
            String name = "testBinlogGlobalToNormal";
            execute(mycatConnection, "CREATE DATABASE db1");


            execute(mycatConnection, "CREATE TABLE db1.`travelrecord_input` (\n" +
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

            execute(mycatConnection, "CREATE TABLE db1.`travelrecord_output` (\n" +
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

            List<String> inputTableNames = Arrays.asList("db1.travelrecord_input");
            List<String> outputTableNames = Arrays.asList("db1.travelrecord_output");
            List<Map<String, Object>> info = executeQuery(mycatConnection, BinlogSyncHint.create(name, inputTableNames, outputTableNames));


            deleteData(mycatConnection, "db1", "travelrecord_input");
            deleteData(mycatConnection, "db1", "travelrecord_output");

            execute(mycatConnection, "insert  into db1.`travelrecord_input`(`id`,`traveldate`,`fee`,`days`,`blob`) values ('1',NULL,NULL,NULL,NULL),('2',NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),('3',NULL,NULL,NULL,NULL);\n");
            execute(mycatConnection, "UPDATE `db1`.`travelrecord_input` SET `fee` = '2' WHERE `id` = '3'; ");
            execute(mycatConnection, "delete from db1.travelrecord_input");

            check(mycatConnection);

            execute(mycatConnection, "insert  into db1.`travelrecord_input`(`id`,`traveldate`,`fee`,`days`,`blob`) values ('4',NULL,NULL,NULL,NULL),('5',NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),('6',NULL,NULL,NULL,NULL);\n");
            execute(mycatConnection, "UPDATE `db1`.`travelrecord_input` SET `fee` = '2' WHERE `id` = '4'; ");

            mycatConnection.setAutoCommit(false);
            execute(mycatConnection, "insert  into db1.`travelrecord_input`(`id`,`traveldate`,`fee`,`days`,`blob`) values ('7',NULL,NULL,NULL,NULL),('8',NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),('9',NULL,NULL,NULL,NULL);\n");
            execute(mycatConnection, "UPDATE `db1`.`travelrecord_input` SET `fee` = '2' WHERE `id` = '5'; ");
            mycatConnection.setAutoCommit(true);

            check(mycatConnection);

            System.out.println();
        }
    }

    @Test
    @Ignore
    public void testBinlogShardingToShardingWithSnapshot() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, BinlogClearHint.create());
            execute(mycatConnection, RESET_CONFIG);
            String name = "testBinlogGlobalToNormal";
            execute(mycatConnection, "CREATE DATABASE db1");


            execute(mycatConnection, "CREATE TABLE db1.`travelrecord_input` (\n" +
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

            execute(mycatConnection, "CREATE TABLE db1.`travelrecord_output` (\n" +
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

            List<String> inputTableNames = Arrays.asList("db1.travelrecord_input");
            List<String> outputTableNames = Arrays.asList("db1.travelrecord_output");
            List<Map<String, Object>> maps = executeQuery(mycatConnection, BinlogSnapshotHint.create(name));
            String id = maps.get(0).get("Id").toString();



            deleteData(mycatConnection, "db1", "travelrecord_input");
            deleteData(mycatConnection, "db1", "travelrecord_output");

            execute(mycatConnection, "insert  into db1.`travelrecord_input`(`id`,`traveldate`,`fee`,`days`,`blob`) values ('1',NULL,NULL,NULL,NULL),('2',NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),('3',NULL,NULL,NULL,NULL);\n");
            execute(mycatConnection, "UPDATE `db1`.`travelrecord_input` SET `fee` = '2' WHERE `id` = '3'; ");
            execute(mycatConnection, "delete from db1.travelrecord_input");

            execute(mycatConnection, "insert  into db1.`travelrecord_input`(`id`,`traveldate`,`fee`,`days`,`blob`) values ('4',NULL,NULL,NULL,NULL),('5',NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),('6',NULL,NULL,NULL,NULL);\n");
            execute(mycatConnection, "UPDATE `db1`.`travelrecord_input` SET `fee` = '2' WHERE `id` = '4'; ");

            mycatConnection.setAutoCommit(false);
            execute(mycatConnection, "insert  into db1.`travelrecord_input`(`id`,`traveldate`,`fee`,`days`,`blob`) values ('7',NULL,NULL,NULL,NULL),('8',NULL,NULL,NULL,NULL),(NULL,NULL,NULL,NULL,NULL),('9',NULL,NULL,NULL,NULL);\n");
            execute(mycatConnection, "UPDATE `db1`.`travelrecord_input` SET `fee` = '2' WHERE `id` = '5'; ");
            mycatConnection.setAutoCommit(true);

            List<Map<String, Object>> info = executeQuery(mycatConnection, BinlogSyncHint.create(name, id,inputTableNames, outputTableNames));

            check(mycatConnection);

            System.out.println();
        }
    }

    private void check(Connection mycatConnection) throws Exception {
        List<Map<String, Object>> first;
        List<Map<String, Object>> second;
        long start = System.currentTimeMillis();
        do {
            TimeUnit.SECONDS.sleep(1);
            first = JdbcUtils.executeQuery(mycatConnection, "select * from `db1`.`travelrecord_input` order by id", Collections.emptyList());
            second = JdbcUtils.executeQuery(mycatConnection, "select * from `db1`.`travelrecord_output` order by id", Collections.emptyList());
            if (first.equals(second)) {
                return;
            }
        } while (System.currentTimeMillis() - start <= TimeUnit.SECONDS.toMillis(10));
        Assert.assertEquals(first,second);
    }
}
