package io.mycat.sql;

import io.mycat.assemble.MycatTest;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.*;
import java.sql.Date;
import java.util.*;

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
            Assert.assertTrue("[{id=1, name=abc, is_enable=1}]".equals(maps.toString())||"[{id=1, name=abc, is_enable=true}]".equals(maps.toString()));
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
            Assert.assertTrue("[{id=1, name=abc, is_enable=1}]".equals(maps.toString())||"[{id=1, name=abc, is_enable=true}]".equals(maps.toString()));
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

            execute(mycatConnection,"CREATE TABLE IF NOT EXISTS `service` (\n" +
                    "  `id` bigint(20) NOT NULL,\n" +
                    "  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,\n" +
                    "  PRIMARY KEY (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");

            execute(mycatConnection,"CREATE TABLE IF NOT EXISTS `user` (\n" +
                    "  `id` bigint(20) NOT NULL,\n" +
                    "  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,\n" +
                    "  PRIMARY KEY (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");

            execute(mycatConnection,"CREATE TABLE cloud.log (\n" +
                    "  `id` BIGINT(20) DEFAULT NULL,\n" +
                    "  `user_id` BIGINT(20) DEFAULT NULL,\n" +
                    "  `service_id` INT(11) DEFAULT NULL,\n" +
                    "  `submit_time` DATETIME DEFAULT NULL\n" +
                    ") ENGINE=INNODB DEFAULT CHARSET=utf8  dbpartition BY YYYYDD(submit_time) dbpartitions 2 tbpartition BY MOD_HASH (id) tbpartitions 8;\n");

            deleteData(mycatConnection, "cloud", "service");
            deleteData(mycatConnection, "cloud", "user");
            deleteData(mycatConnection, "cloud", "log");

            String sql1="SELECT log.id AS log_id,user.name AS user_name, service.name AS service_name,log.submit_time\n" +
                    "FROM\n" +
                    "`cloud`.`log` INNER JOIN `cloud`.`user`\n" +
                    "ON log.user_id = user.id\n" +
                    "INNER JOIN `cloud`.`service`\n" +
                    "ON service.id  = service_id\n" +
                    "ORDER BY log.submit_time DESC LIMIT 0,20;";
            System.out.println(sql1);
            String explain1 = explain(mycatConnection, sql1);
            System.out.println(explain1);
            executeQuery(mycatConnection,sql1);

            Assert.assertTrue(explain1.contains("MycatView(distribution=[[cloud.log]]"));

            String sql2 = "SELECT log.id AS log_id,user.name AS user_name, service.name AS service_name,log.submit_time FROM (SELECT log.`id` ,log.`service_id`,log.`submit_time`,log.`user_id` FROM `cloud`.`log`  WHERE log.submit_time = '2021-5-31' ORDER BY log.submit_time DESC LIMIT 0,20) AS `log` INNER JOIN `cloud`.`user` ON log.user_id = user.id INNER JOIN `cloud`.`service`  ON service.id  = log.service_id ORDER BY log.submit_time DESC LIMIT 0,20;";
            System.out.println(sql2);
            String explain2 = explain(mycatConnection, sql2);
            System.out.println(explain2);
            executeQuery(mycatConnection,sql2);
            Assert.assertTrue(explain2.contains("WHERE (`submit_time` = ?) ORDER BY (`submit_time` IS NULL) DESC, `submit_time` DESC LIMIT 20 OFFSET 0)"));

            //test transaction
            mycatConnection.setAutoCommit(false);
            executeQuery(mycatConnection,sql2);
            mycatConnection.setAutoCommit(true);
        }
    }
}
