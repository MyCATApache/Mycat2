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
}
