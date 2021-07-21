package io.mycat.sql;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import io.mycat.config.ShardingBackEndTableInfoConfig;
import io.mycat.config.ShardingFuntion;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.hint.CreateTableHint;
import io.mycat.router.mycat1xfunction.PartitionByFileMap;
import io.mycat.router.mycat1xfunction.PartitionByHotDate;
import io.mycat.router.mycat1xfunction.PartitionByRangeMod;
import org.apache.groovy.util.Maps;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class UserCaseTest implements MycatTest {

    @Test
    public void case1() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");


            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));

            execute(mycatConnection, "USE `db1`;");

            execute(mycatConnection, "CREATE TABLE `travelrecord2` (\n" +
                    "  `id` bigint(20) NOT NULL KEY,\n" +
                    "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                    "  `traveldate` datetime(6) DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int(11) DEFAULT NULL,\n" +
                    "  `blob` longblob DEFAULT NULL\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "tbpartition by YYYYMM(traveldate) tbpartitions 12;");

            execute(mycatConnection, "CREATE TABLE `user` (\n" +
                    "  `id` int NOT NULL,\n" +
                    "  `name` varchar(45) DEFAULT NULL,\n" +
                    "  PRIMARY KEY (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");

            deleteData(mycatConnection, "db1", "travelrecord2");

            executeQuery(mycatConnection, "SELECT t1.id,t1.name,t2.count FROM db1.user as t1\n" +
                    "left join (select count(1) as `count`,`user_id` from travelrecord2 group by `user_id`) \n" +
                    "as t2 on `t1`.`id` = `t2`.`user_id`;");

            executeQuery(mycatConnection, "SELECT t1.id,t1.name,t2.count FROM db1.user as t1\n" +
                    "left join (select count(1) as `count`,`user_id` from travelrecord2 group by `user_id`) \n" +
                    "as `t2` on `t1`.`id` = `t2`.`user_id`;");
            execute(mycatConnection, "use db1");

            execute(mycatConnection, "START TRANSACTION;\n" +
                    "INSERT INTO `travelrecord2` (id,`blob`, `days`, `fee`, `traveldate`, `user_id`)\n" +
                    "VALUES (1,NULL, 3, 3, timestamp('2021-02-21 12:23:42.058156'), 'tom');\n" +
                    "SELECT ROW_COUNT();\n" +
                    "COMMIT;");
            execute(mycatConnection, "" +
                    "UPDATE  `travelrecord2` SET id = 1 where id = 1;" +
                    "SELECT ROW_COUNT();\n");
            execute(mycatConnection, "" +
                    "UPDATE  `user` SET id = 1 where id = 1;SELECT * from `user`; " +
                    "SELECT ROW_COUNT();\n");
            executeQuery(mycatConnection, "SELECT ROW_COUNT();");
            deleteData(mycatConnection, "db1", "travelrecord2");
            execute(mycatConnection, "INSERT INTO `travelrecord2`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`)\n" +
                    "VALUES (1,2,timestamp('2021-02-22 18:34:05.983692'),3.5,4,NULL)");

            Statement statement = mycatConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT traveldate FROM travelrecord2 WHERE id = 1;");
            resultSet.next();
            Timestamp traveldate = resultSet.getTimestamp("traveldate");
//            Assert.assertEquals(Timestamp.valueOf("2021-02-23 10:34:05.983692"), traveldate);
            resultSet.close();
//            = executeQuery(mycatConnection, );
//            Assert.assertTrue(maps.get(0).get().toString().endsWith("983692"));//!= 05.983692000
            List<Map<String, Object>> maps;
            maps = executeQuery(mycatConnection, "SELECT * FROM travelrecord2 WHERE traveldate = '2021-02-22 18:34:05.983692';");
//            Assert.assertTrue(!maps.isEmpty());
            maps = executeQuery(mycatConnection, "SELECT * FROM travelrecord2 WHERE traveldate = timestamp('2021-02-22 18:34:05.983692');");
//            Assert.assertTrue(!maps.isEmpty());
            maps = executeQuery(mycatConnection, "SELECT * FROM travelrecord2 WHERE CONVERT(traveldate,date) = '2021-2-22';");
            execute(mycatConnection, "START TRANSACTION\n" +
                    "INSERT INTO `travelrecord2`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`)\n" +
                    "VALUES \n" +
                    "(6,2,timestamp('2021-02-22 18:34:05.983692'),4.5,4,NULL),\n" +
                    "(7,2,timestamp('2021-02-22 18:34:05.983692'),4.5,4,NULL),\n" +
                    "(8,2,timestamp('2021-02-22 18:34:05.983692'),4.5,4,NULL);\n" +
                    "COMMIT;");
            deleteData(mycatConnection, "db1", "travelrecord2");
            execute(mycatConnection, "START TRANSACTION\n" +
                    "INSERT INTO `travelrecord2`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`)\n" +
                    "VALUES \n" +
                    "(6,2,'2021-02-22 18:34:05.983692',4.5,4,NULL),\n" +
                    "(7,2,'2021-02-22 18:34:05.983692',4.5,4,NULL),\n" +
                    "(8,2,'2021-02-22 18:34:05.983692',4.5,4,NULL);\n" +
                    "COMMIT;");
        }
    }

    @Test
    public void case2() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection mysqlConnection = getMySQLConnection(DB1)) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");


            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));

            execute(mycatConnection, "USE `db1`;");

            execute(mycatConnection, "CREATE TABLE `user` (\n" +
                    "  `id` int NOT NULL,\n" +
                    "  `name` varchar(45) DEFAULT NULL,\n" +
                    "\t`is_enable` tinyint(1) not null default 1,\n" +
                    "  PRIMARY KEY (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");
            deleteData(mycatConnection, "db1", "user");
            execute(mycatConnection, "insert into `user`(`id`,`name`,`is_enable`) values (1,'abc',1);");
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "SELECT * FROM `user`;");//[{id=1, name=abc, is_enable=1}]
            List<Map<String, Object>> right = executeQuery(mysqlConnection, "SELECT * FROM db1.`user`;");//[{id=1, name=abc, is_enable=true}]
            Assert.assertTrue("[{id=1, name=abc, is_enable=1}]".equals(maps.toString()) || "[{id=1, name=abc, is_enable=true}]".equals(maps.toString()));
//            Assert.assertArrayEquals(new byte[]{1}, ((byte[]) maps.get(0).get("is_enable")));
        }
    }


    @Test
    public void case3() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection mysqlConnection = getMySQLConnection(DB1)) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");


            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));

            execute(mycatConnection, "USE `db1`;");

            execute(mycatConnection, "CREATE TABLE `user` (\n" +
                    "  `id` int NOT NULL,\n" +
                    "  `name` varchar(45) DEFAULT NULL,\n" +
                    "\t`is_enable` bit(1) not null default 1,\n" +
                    "  PRIMARY KEY (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");
            deleteData(mycatConnection, "db1", "user");
            execute(mycatConnection, "insert into `user`(`id`,`name`,`is_enable`) values (1,'abc',1);");
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "SELECT * FROM `user`;");//[{id=1, name=abc, is_enable=1}]
            List<Map<String, Object>> right = executeQuery(mysqlConnection, "SELECT * FROM db1.`user`;");//[{id=1, name=abc, is_enable=true}]
            Assert.assertTrue("[{id=1, name=abc, is_enable=1}]".equals(maps.toString()) || "[{id=1, name=abc, is_enable=true}]".equals(maps.toString()));
        }
    }

    @Test
    public void case4() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE cloud");


            execute(mycatConnection, "CREATE DATABASE cloud");


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

            execute(mycatConnection, "USE `cloud`;");

            execute(mycatConnection, "CREATE TABLE IF NOT EXISTS `service` (\n" +
                    "  `id` bigint(20) NOT NULL,\n" +
                    "  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,\n" +
                    "  PRIMARY KEY (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");

            execute(mycatConnection, "CREATE TABLE IF NOT EXISTS `user` (\n" +
                    "  `id` bigint(20) NOT NULL,\n" +
                    "  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,\n" +
                    "  PRIMARY KEY (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");

            execute(mycatConnection, "CREATE TABLE cloud.log (\n" +
                    "  `id` BIGINT(20) DEFAULT NULL,\n" +
                    "  `user_id` BIGINT(20) DEFAULT NULL,\n" +
                    "  `service_id` INT(11) DEFAULT NULL,\n" +
                    "  `submit_time` DATETIME DEFAULT NULL\n" +
                    ") ENGINE=INNODB DEFAULT CHARSET=utf8  dbpartition BY YYYYDD(submit_time) dbpartitions 2 tbpartition BY MOD_HASH (id) tbpartitions 8;\n");

            deleteData(mycatConnection, "cloud", "service");
            deleteData(mycatConnection, "cloud", "user");
            deleteData(mycatConnection, "cloud", "log");

            String sql1 = "SELECT log.id AS log_id,user.name AS user_name, service.name AS service_name,log.submit_time\n" +
                    "FROM\n" +
                    "`cloud`.`log` INNER JOIN `cloud`.`user`\n" +
                    "ON log.user_id = user.id\n" +
                    "INNER JOIN `cloud`.`service`\n" +
                    "ON service.id  = service_id\n" +
                    "ORDER BY log.submit_time DESC LIMIT 0,20;";
            System.out.println(sql1);
            String explain1 = explain(mycatConnection, sql1);
            System.out.println(explain1);
            executeQuery(mycatConnection, sql1);

            Assert.assertTrue(explain1.contains("MycatView(distribution=[[cloud.log]]"));

            // String sql2 = "/*+MYCAT:use_values_join(log,user) use_values_join(log,service) */ SELECT log.id AS log_id,user.name AS user_name, service.name AS service_name,log.submit_time FROM (SELECT log.`id` ,log.`service_id`,log.`submit_time`,log.`user_id` FROM `cloud`.`log`  WHERE log.submit_time = '2021-5-31' ORDER BY log.submit_time DESC LIMIT 0,20) AS `log` INNER JOIN `cloud`.`user` ON log.user_id = user.id INNER JOIN `cloud`.`service`  ON service.id  = log.service_id ORDER BY log.submit_time DESC LIMIT 0,20;";
            String sql2 = " SELECT log.id AS log_id,user.name AS user_name, service.name AS service_name,log.submit_time FROM (SELECT log.`id` ,log.`service_id`,log.`submit_time`,log.`user_id` FROM `cloud`.`log`  WHERE log.submit_time = '2021-5-31' ORDER BY log.submit_time DESC LIMIT 0,20) AS `log` INNER JOIN `cloud`.`user` ON log.user_id = user.id INNER JOIN `cloud`.`service`  ON service.id  = log.service_id ORDER BY log.submit_time DESC LIMIT 0,20;";

            System.out.println(sql2);
            String explain2 = explain(mycatConnection, sql2);
            System.out.println(explain2);
            Assert.assertEquals(true, explain2.contains("TableLook"));
            executeQuery(mycatConnection, sql2);

            //test transaction
            mycatConnection.setAutoCommit(false);
            executeQuery(mycatConnection, sql2);
            mycatConnection.setAutoCommit(true);
        }
    }

    @Test
    public void test548() throws Exception {
        try (Connection mycat = getMySQLConnection(DB_MYCAT);) {
            execute(mycat, RESET_CONFIG);
            String db = "db1";
            String tableName = "sharding";
            execute(mycat, "drop database if EXISTS " + db);
            execute(mycat, "create database " + db);
            execute(mycat, "use " + db);


            execute(mycat, CreateClusterHint
                    .create("c0", Arrays.asList("prototypeDs"), Arrays.asList()));

            execute(
                    mycat,
                    CreateTableHint
                            .createSharding(db, tableName,
                                    "CREATE TABLE db1.`sharding` (\n" +
                                            "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                                            "  `user_id` varchar(100) DEFAULT NULL,\n" +
                                            "  `create_time` date DEFAULT NULL,\n" +
                                            "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                                            "  `days` int DEFAULT NULL,\n" +
                                            "  `blob` longblob,\n" +
                                            "  PRIMARY KEY (`id`),\n" +
                                            "  KEY `id` (`id`)\n" +
                                            ") ENGINE=InnoDB  DEFAULT CHARSET=utf8",
                                    ShardingBackEndTableInfoConfig.builder()
                                            .schemaNames(db)
                                            .tableNames("sharding_0,sharding_1")
                                            .targetNames("c0").build(),
                                    ShardingFuntion.builder()
                                            .clazz(PartitionByHotDate.class.getCanonicalName())
                                            .properties(Maps.of(
                                                    "dateFormat", "yyyy-MM-dd",
                                                    "lastTime", 90,
                                                    "partionDay", 180,
                                                    "columnName", "create_time"
                                            )).build())
            );
            deleteData(mycat, db, tableName);
            execute(mycat, "insert into " + tableName + " (create_time) VALUES ('2021-06-30')");
            execute(mycat, "insert into " + tableName + "(create_time) VALUES ('2021-06-29')");
            execute(mycat, "insert into " + tableName + " (create_time) VALUES ('2021-06-29')");
            List<Map<String, Object>> maps = executeQuery(mycat, "select * from db1.sharding");
            Assert.assertEquals(3, maps.size());
            List<Map<String, Object>> maps1 = executeQuery(mycat, "select * from db1.sharding where create_time = '2021-06-30'");
            Assert.assertEquals(1, maps1.size());
            System.out.println();

//            execute(mycat, "drop database " + db);
        }
    }

    //测试 `1cloud` 标识符
    //测试 预处理 id = 1
    //测试bit tiny类型
    @Test
    public void case5() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT)) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE `1cloud`");


            execute(mycatConnection, "CREATE DATABASE `1cloud`");


            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection, CreateDataSourceHint
                    .create("ds1",
                            DB2));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));

            execute(mycatConnection, "USE `1cloud`;");

            execute(mycatConnection, "CREATE TABLE IF NOT EXISTS `1service` (\n" +
                    "  `b` bit(1) NOT NULL,\n" +
                    "  `tiny` TINYINT(4)," +
                    " `s` varchar(20) NOT NULL,\n" +
                    "  PRIMARY KEY (`tiny`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");

            execute(mycatConnection, "CREATE TABLE `1cloud`.`1log` (\n" +
                    "  `id` BIGINT(20) DEFAULT NULL,\n" +
                    "  `user_id` BIGINT(20) DEFAULT NULL,\n" +
                    "  `service_id` INT(11) DEFAULT NULL,\n" +
                    "  `submit_time` DATETIME DEFAULT NULL\n" +
                    ") ENGINE=INNODB DEFAULT CHARSET=utf8  dbpartition BY YYYYDD(submit_time) dbpartitions 1 tbpartition BY MOD_HASH (id) tbpartitions 1;\n");
            deleteData(mycatConnection, "`1cloud`", "`1service`");
            deleteData(mycatConnection, "`1cloud`", "`1log`");
            count(mycatConnection, "`1cloud`", "`1service`");
            count(mycatConnection, "`1cloud`", "`1log`");
            execute(mycatConnection, "insert `1cloud`.`1log` (id) values (1)");
            execute(mycatConnection, "insert `1cloud`.`1service`  values (1,1,'2')");
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mycatConnection, "select * from `1cloud`.`1log` where id = ?", Arrays.asList(1L));
            Assert.assertEquals(1, maps.size());
            List<Map<String, Object>> maps2 = JdbcUtils.executeQuery(mycatConnection, "select * from `1cloud`.`1service`", Collections.emptyList());
            Assert.assertEquals(3, maps2.get(0).size());
            Assert.assertEquals("[{b=true, tiny=1, s=2}]", maps2.toString());
            System.out.println();
        }
    }

    @Test
    public void case6() throws Exception {
        String table = "sharding";
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT)) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE `1cloud`");


            execute(mycatConnection, "CREATE DATABASE `1cloud`");



            execute(mycatConnection, "USE `1cloud`;");

            execute(
                    mycatConnection,
                    CreateTableHint
                            .createSharding("1cloud", table,
                                    "create table " + table + "(\n" +
                                            "id int(11) NOT NULL AUTO_INCREMENT,\n" +
                                            "user_id int(11) ,\n" +
                                            "user_name varchar(128), \n" +
                                            "PRIMARY KEY (`id`) \n" +
                                            ")ENGINE=InnoDB DEFAULT CHARSET=utf8 ",
                                    ShardingBackEndTableInfoConfig.builder()
                                            .schemaNames("c")
                                            .tableNames("file_$0-2")
                                            .targetNames("prototype").build(),
                                    ShardingFuntion.builder()
                                            .clazz(PartitionByFileMap.class.getCanonicalName())
                                            .properties(Maps.of(
                                                    "defaultNode", "0",
                                                    "type", "Integer",
                                                    "columnName", "id"
                                            )).ranges(Maps.of(
                                            "130100", "0",
                                            "130200", "1",
                                            "130300", "2"
                                    )).build())
            );
            deleteData(mycatConnection, "`1cloud`", table);
            Assert.assertEquals(0, count(mycatConnection, "`1cloud`", table));
            execute(mycatConnection, "insert `1cloud`." +
                    table +
                    " (id) values (130100)");
            Assert.assertEquals(1, count(mycatConnection, "`1cloud`", table));
            String zero_w = explain(mycatConnection, "insert `1cloud`." +
                    table +
                    " (id) values (130100)");
            String one_w = explain(mycatConnection, "insert `1cloud`." +
                    table +
                    " (id) values (130200)");
            String second_r = explain(mycatConnection, "insert `1cloud`." +
                    table +
                    " (id) values (130300)");

            Assert.assertTrue(zero_w.contains("file_0"));
            Assert.assertTrue(one_w.contains("file_1"));
            Assert.assertTrue(second_r.contains("file_2"));

            String zero_r = explain(mycatConnection, "select * from " + table + " where id = " + 130100);
            String one_r = explain(mycatConnection, "select * from " + table + " where id = " + 130200);
            String two_r = explain(mycatConnection, "select * from " + table + " where id = " + 130300);

            Assert.assertTrue(zero_r.contains("file_0"));
            Assert.assertTrue(one_r.contains("file_1"));
            Assert.assertTrue(two_r.contains("file_2"));

            System.out.println();
        }
    }

        @Test
        public void case7() throws Exception {
            String table = "sharding";
            try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT)) {
                execute(mycatConnection, RESET_CONFIG);
                execute(mycatConnection,
                        CreateClusterHint.create("c0",
                                Arrays.asList("prototypeDs"), Collections.emptyList()));
                execute(mycatConnection, "DROP DATABASE `1cloud`");


                execute(mycatConnection, "CREATE DATABASE `1cloud`");


                execute(mycatConnection, "USE `1cloud`;");

                execute(mycatConnection, "CREATE TABLE `1cloud`.`1log` (\n" +
                        "  `id` BIGINT(20) DEFAULT NULL,\n" +
                        "  `user_id` BIGINT(20) DEFAULT NULL,\n" +
                        "  `service_id` INT(11) DEFAULT NULL,\n" +
                        "  `submit_time` DATETIME DEFAULT NULL\n" +
                        ") ENGINE=INNODB DEFAULT CHARSET=utf8  dbpartition BY YYYYDD(submit_time) dbpartitions 1 tbpartition BY MOD_HASH (id) tbpartitions 1;\n");


                String sql = "select any_value(submit_time) from `1log` where submit_time between '2019-5-31' and '2019-6-21' group by DATE_FORMAT(submit_time,'%Y-%m')";
                String explain = explain(mycatConnection,sql );
                executeQuery(mycatConnection,sql);
                System.out.println();
            }
    }
}
