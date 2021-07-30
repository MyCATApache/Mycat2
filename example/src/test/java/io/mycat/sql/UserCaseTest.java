package io.mycat.sql;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.ShardingBackEndTableInfoConfig;
import io.mycat.config.ShardingFuntion;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.hint.CreateTableHint;
import io.mycat.router.mycat1xfunction.PartitionByFileMap;
import io.mycat.router.mycat1xfunction.PartitionByHotDate;
import io.mycat.util.ByteUtil;
import org.apache.groovy.util.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.boot.autoconfigure.quartz.QuartzProperties;

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


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
            String explain = explain(mycatConnection, sql);
            executeQuery(mycatConnection, sql);
            System.out.println();
            execute(mycatConnection, "USE `1cloud`;");
            execute(
                    mycatConnection,
                    CreateTableHint
                            .createSharding("1cloud", "stat_ad_sdk",
                                    "CREATE TABLE stat_ad_sdk (\n" +
                                            "ad_id int unsigned DEFAULT NULL COMMENT '广告位id',\n" +
                                            "ad_uuid varchar(50) DEFAULT NULL COMMENT '渠道广告位id',\n" +
                                            "ad_name varchar(500) DEFAULT NULL COMMENT '广告位名称',\n" +
                                            "ad_type varchar(50) DEFAULT NULL COMMENT '广告位类型',\n" +
                                            "uid int unsigned DEFAULT NULL COMMENT '开发者id',\n" +
                                            "user_name varchar(500) DEFAULT NULL COMMENT '开发者名',\n" +
                                            "app_id int unsigned DEFAULT NULL COMMENT '应用id',\n" +
                                            "app_name varchar(500) DEFAULT NULL COMMENT '应用名',\n" +
                                            "channel_id int unsigned DEFAULT NULL COMMENT '渠道id',\n" +
                                            "date date DEFAULT NULL COMMENT '日期',\n" +
                                            "req int unsigned NOT NULL DEFAULT '0' COMMENT '请求量',\n" +
                                            "fill_rate varchar(20) DEFAULT NULL COMMENT '填充率',\n" +
                                            "`show` int unsigned NOT NULL DEFAULT '0' COMMENT '展示量',\n" +
                                            "click int unsigned NOT NULL DEFAULT '0' COMMENT '点击量',\n" +
                                            "video_error int unsigned NOT NULL DEFAULT '0' COMMENT '视频播放错误量',\n" +
                                            "video_not_complete int unsigned NOT NULL DEFAULT '0' COMMENT '视频未完整播放量',\n" +
                                            "version varchar(50) DEFAULT NULL COMMENT 'sdk版本',\n" +
                                            "UNIQUE KEY dateAdIdVer (date,ad_uuid,version) USING BTREE\n" +
                                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COMMENT='广告sdk日统计数据';",
                                    ShardingBackEndTableInfoConfig.builder()
                                            .schemaNames("c")
                                            .tableNames("stat_ad_sdk_$0-11")
                                            .targetNames("prototype").build(),
                                    ShardingFuntion.builder()
                                            .clazz(io.mycat.router.mycat1xfunction.PartitionByMonth.class.getCanonicalName())
                                            .properties(Maps.of(
                                                    "beginDate", "2019-01-01",
                                                    "endDate", "2099-12-01",
                                                    "dateFormat", "yyyy-MM-dd",
                                                    "columnName", "date"
                                            )).build())
            );

            sql = "select any_value(date) from `stat_ad_sdk` where date between '2019-5-01' and '2019-05-31' group by DATE_FORMAT(date,'%Y-%m')";
            explain = explain(mycatConnection, sql);
            executeQuery(mycatConnection, sql);
            System.out.println();

        }
    }

    @Test
    public void case8() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection mysqlConnection = getMySQLConnection(DB1)) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");
            execute(mycatConnection, "use db1");
            execute(mycatConnection, "CREATE TABLE `sys_menu` (\n" +
                    "  `menu_id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
                    "  `menu_name` varchar(50) NOT NULL ,\n" +
                    "  `parent_id` bigint(20) DEFAULT '0' ,\n" +
                    "  `order_num` int(4) DEFAULT '0',\n" +
                    "  `path` varchar(200) DEFAULT '' ,\n" +
                    "  `component` varchar(255) DEFAULT NULL ,\n" +
                    "  `is_frame` int(1) DEFAULT '1' ,\n" +
                    "  `is_cache` int(1) DEFAULT '0',\n" +
                    "  `menu_type` varchar(1) DEFAULT '' ,\n" +
                    "  `visible` varchar(1) DEFAULT '0',\n" +
                    "  `status` varchar(1) DEFAULT '0' ,\n" +
                    "  `perms` varchar(100) DEFAULT NULL ,\n" +
                    "  `icon` varchar(100) DEFAULT '#' ,\n" +
                    "  `create_by` varchar(64) DEFAULT '' ,\n" +
                    "  `create_time` datetime DEFAULT NULL ,\n" +
                    "  `update_by` varchar(64) DEFAULT '',\n" +
                    "  `update_time` datetime DEFAULT NULL,\n" +
                    "  `remark` varchar(500) DEFAULT '',\n" +
                    "  PRIMARY KEY (`menu_id`)\n" +
                    ") ENGINE=InnoDB AUTO_INCREMENT=1080 DEFAULT CHARSET=utf8 ;");
            deleteData(mycatConnection, "db1", "sys_menu");
            execute(mycatConnection, "INSERT INTO `sys_menu` VALUES ('1', '系统管理', '0', '6', 'common', null, '1', '0', 'M', '0', '0', '', 'build', 'admin', '2021-04-15 12:06:30', 'admin', null, '系统管理目录');");
            String sql = "select * from db1.sys_menu";

            Statement mycatStatement = mycatConnection.createStatement();
            ResultSet mycatresultSet = mycatStatement.executeQuery(sql);
            ResultSetMetaData mycatmetaData = mycatresultSet.getMetaData();

            Statement mysqlstatement = mysqlConnection.createStatement();
            ResultSet mysqlresultSet = mysqlstatement.executeQuery(sql);
            ResultSetMetaData mysqlmetaData = mysqlresultSet.getMetaData();

            Assert.assertEquals(mysqlmetaData.getColumnCount(), mycatmetaData.getColumnCount());
            for (int i = 1; i <= mysqlmetaData.getColumnCount(); i++) {
                int mysqlcolumnType = mysqlmetaData.getColumnType(i);
                int mysqlNullable = mysqlmetaData.isNullable(i);
                int mycatcolumnType = mycatmetaData.getColumnType(i);
                int mycatNullable = mycatmetaData.isNullable(i);
                Assert.assertEquals(mysqlcolumnType, mycatcolumnType);
                Assert.assertEquals(mysqlNullable, mycatNullable);
            }
            System.out.println();
        }
    }

    @Test
    public void case9() throws Exception {
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


            testBlob(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint(20) NOT NULL KEY,\n" +
                    "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                    "  `traveldate` datetime(6) DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int(11) DEFAULT NULL,\n" +
                    "  `blob` longblob DEFAULT NULL\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "tbpartition by mod_hash(id) tbpartitions 1;");
            testBlob(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint(20) NOT NULL KEY,\n" +
                    "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                    "  `traveldate` datetime(6) DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int(11) DEFAULT NULL,\n" +
                    "  `blob` longblob DEFAULT NULL\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "");
        }
    }

    @Test
    public void case10() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT)) {


            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");

            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));


            testBlob(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint(20) NOT NULL KEY,\n" +
                    "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                    "  `traveldate` datetime(6) DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int(11) DEFAULT NULL,\n" +
                    "  `blob` longblob DEFAULT NULL\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "tbpartition by mod_hash(id) tbpartitions 1;");
            testBlob(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint(20) NOT NULL KEY,\n" +
                    "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                    "  `traveldate` datetime(6) DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int(11) DEFAULT NULL,\n" +
                    "  `blob` longblob DEFAULT NULL\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "");
        }
    }

    @Test
    public void case11() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT)) {


            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");

            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));


            testBlob(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint(20) NOT NULL KEY,\n" +
                    "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                    "  `traveldate` datetime(6) DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int(11) DEFAULT NULL,\n" +
                    "  `blob` longblob DEFAULT NULL\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "tbpartition by mod_hash(id) tbpartitions 1;");
            String explain = explain(mycatConnection, "select count(*) from db1.travelrecord");
            Assert.assertTrue(explain.contains("MycatHashAggregate(group=[{}], count(*)=[$SUM0($0)])\n" +
                    "  MycatView(distribution=[[db1.travelrecord]])\n" +
                    "Each(targetName=c0, sql=SELECT COUNT(*) AS `count(*)` FROM db1_0.travelrecord_0 AS `travelrecord`)"));
            System.out.println();
        }
    }

    /**
     * 测试数据源更新
     * @throws Exception
     */
    @Test
    public void case12() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection mysql2 = getMySQLConnection(DB2);
        ) {
            execute(mycatConnection, RESET_CONFIG);

            DatasourceConfig ds0 = new DatasourceConfig();
            ds0.setName("ds0");
            ds0.setUrl(DB1);
            ds0.setPassword(CreateDataSourceHint.PASSWORD);
            ds0.setUser(CreateDataSourceHint.USER_NAME);
            ds0.setInstanceType("READ_WRITE");

            DatasourceConfig ds1 = new DatasourceConfig();
            ds1.setName("ds1");
            ds1.setUrl(DB2);
            ds1.setPassword(CreateDataSourceHint.PASSWORD);
            ds1.setUser(CreateDataSourceHint.USER_NAME);
            ds1.setInstanceType("READ_WRITE");

            execute(mycatConnection, CreateDataSourceHint
                    .create(ds0));

            execute(mycatConnection, CreateDataSourceHint
                    .create(ds1));

            execute(mycatConnection,
                    CreateClusterHint.create("prototype",
                            Arrays.asList("ds0"), Arrays.asList("ds1")));

            execute(mycatConnection, "CREATE DATABASE IF NOT EXISTS db1");
            execute(mycatConnection, "CREATE TABLE IF NOT EXISTS db1.`tbl`(\n" +
                    "   `id` INT UNSIGNED AUTO_INCREMENT," +
                    "   PRIMARY KEY ( `id` )\n" +
                    ")ENGINE=InnoDB DEFAULT CHARSET=utf8;");


            ds0.setInstanceType("WRITE");
            ds1.setInstanceType("READ");


            execute(mycatConnection, CreateDataSourceHint
                    .create(ds0));

            execute(mycatConnection, CreateDataSourceHint
                    .create(ds1));


            execute(mycatConnection,
                    CreateClusterHint.create("prototype",
                            Arrays.asList("ds0"), Arrays.asList("ds1")));
            deleteData(mysql2,"db1","tbl");
            execute(mysql2, "INSERT INTO db1.tbl \n" +
                    "(id)\n" +
                    "VALUES\n" +
                    " (1);");

            long now = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1);

            while (System.currentTimeMillis() > now) {
                if (!hasData(mycatConnection, "db1", "tbl")) {
                    Assert.fail();
                }
            }
        }
    }
    /**
     * 测试数据源更新
     * @throws Exception
     */
    @Test
    public void case13() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT_PSTMT);) {
            execute(mycatConnection, RESET_CONFIG);
            JdbcUtils.execute(mycatConnection,"CREATE TABLE `testblob` (\n" +
                    "  `id` bigint(20) DEFAULT NULL,\n" +
                    "  `data` blob\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
            deleteData(mycatConnection,"mysql","testblob");
            byte[] data = ByteBuffer.allocate(8).putLong(Long.MAX_VALUE).array();
            JdbcUtils.execute(mycatConnection, "INSERT INTO mysql.testblob \n" +
                    "(id,data)\n" +
                    "VALUES\n" +
                    " (1,?);",Arrays.asList(data));
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mycatConnection, "select * from mysql.testblob where id = 1", Collections.emptyList());
            byte[] data1 = (byte[])maps.get(0).get("data");
            Assert.assertTrue(Arrays.equals(data,data1));
            System.out.println();
        }
    }

    private void testBlob(Connection mycatConnection, String createTableSQL) throws Exception {
        execute(mycatConnection, createTableSQL);

        deleteData(mycatConnection, "db1", "travelrecord");

        String text = "一二三四五六七八";
        byte[] testData = ByteUtil.getBytes(text, "UTF8");
        JdbcUtils.execute(mycatConnection, " INSERT INTO `db1`.`travelrecord` (`id`, `blob`) VALUES ('1', ?)", Arrays.asList(testData));
        Statement statement = mycatConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("select `blob` from `db1`.`travelrecord` where id = 1");
        ResultSetMetaData metaData = resultSet.getMetaData();
        boolean next = resultSet.next();
        Object bytes = resultSet.getObject(1);
        Assert.assertEquals(text, new String((byte[]) bytes, StandardCharsets.UTF_8));
        System.out.println();
    }

}
