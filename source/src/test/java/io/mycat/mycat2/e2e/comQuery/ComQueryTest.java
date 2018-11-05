package io.mycat.mycat2.e2e.comQuery;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by linxiaofang on 2018/11/5.
 * create database db1;
 * create table travelrecord (id bigint not null primary key,user_id varchar(100),traveldate DATE, fee decimal,days int);
 */
public class ComQueryTest {
    //3306
    //8066
    final static String URL = "jdbc:mysql://10.4.40.57:3306/db1?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC" +
            "&useLocalSessionState=true&failOverReadOnly=false" +
            "&rewriteBatchedStatements=true" +
            "&allowMultiQueries=true" +
            "&useCursorFetch=true";
    final static String USERNAME = "root";
    final static String PASSWORD = "Marble@dls16";

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testShowTableStatus() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.executeQuery("SHOW TABLE STATUS;");
                    statement.executeQuery("SHOW TABLE STATUS FROM db1;");
                    statement.executeQuery("SHOW TABLE STATUS IN db1;");
                    statement.executeQuery("SHOW TABLE STATUS LIKE 'travel%';");
                    statement.executeQuery("SHOW TABLE STATUS WHERE Name LIKE 'travel%';");
                    statement.executeQuery("SHOW TABLE STATUS FROM db1 WHERE Name LIKE 'travel%';");
                    statement.executeQuery("SHOW TABLE STATUS IN db1 WHERE Engine='InnoDB';");
                }
        );
    }

    public static void testShowTriggers() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.executeQuery("SHOW TRIGGERS;");
                    statement.executeQuery("SHOW TRIGGERS FROM `db1`;");
                    statement.executeQuery("SHOW TRIGGERS IN `db1`;");
                    statement.executeQuery("SHOW TRIGGERS LIKE 'acc%';");
                    statement.executeQuery("SHOW TRIGGERS WHERE `Trigger` LIKE 'acc%';");
                    statement.executeQuery("SHOW TRIGGERS FROM `db1` WHERE `Trigger` LIKE 'acc%';");
                    statement.executeQuery("SHOW TRIGGERS IN `db1` WHERE `Trigger` LIKE 'acc%';");
                }
        );
    }

    /*
     * 需要把/tmp/loaddata.txt上传到mysql所在机器的目录下
     */
    public static void testLoadData() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.executeQuery("TRUNCATE TABLE `db1`.`travelrecord`;");
                    statement.executeQuery("LOAD DATA INFILE '/tmp/loaddata.txt' INTO TABLE `db1`.`travelrecord` FIELDS TERMINATED BY ',';");
                }
        );
    }

    public static void testSetOption() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.executeQuery("SET @Name=23");
                }
        );
    }

    public static void testLockUnlock() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.executeQuery("LOCK TABLES `db1`.`travelrecord` READ;");
                    statement.executeQuery("SELECT COUNT(*) FROM `db1`.`travelrecord`;");
                    statement.executeQuery("UNLOCK TABLES;");
                }
        );
    }

    public static void testGrant() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("DROP USER 'jeffrey'@'localhost';");
                    statement.execute("CREATE USER 'jeffrey'@'localhost' IDENTIFIED BY 'What?2018';");
                    statement.execute("GRANT ALL ON db1.* TO 'jeffrey'@'localhost';");
                }
        );
    }

    public static void testChangeDb() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("USE `db1`;");
                }
        );
    }

    public static void testCreateDb() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("CREATE DATABASE IF NOT EXISTS `menagerie`;");
                }
        );
    }

    public static void testDropDb() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("DROP DATABASE IF EXISTS `menagerie`;");
                }
        );
    }

    public static void testAlterDb() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("ALTER TABLE `db1`.`travelrecord` ADD i INT FIRST;");
                    statement.execute("ALTER TABLE `db1`.`travelrecord` DROP i;");
                }
        );
    }

    public static void testRepair() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("REPAIR TABLE `db1`.`travelrecord`;");
                }
        );
    }

    public static void testReplace() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("REPLACE INTO `db1`.`travelrecord` (`id`, `user_id`, `traveldate`, `fee`, `days`) VALUES (3, '2', '2018-11-02', '2', '2') ;");
                }
        );
    }

    public static void testReplaceSelect() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("CREATE TABLE IF NOT EXISTS `db1`.`travelrecord2` (\n" +
                            "  `id` bigint(20) NOT NULL,\n" +
                            "  `user_id` varchar(100) DEFAULT NULL,\n" +
                            "  `traveldate` date DEFAULT NULL,\n" +
                            "  `fee` decimal(10,0) unsigned DEFAULT NULL,\n" +
                            "  `days` int(11) DEFAULT NULL,\n" +
                            "  PRIMARY KEY (`id`)\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");
                    statement.execute("REPLACE INTO `db1`.`travelrecord2` SELECT * FROM `db1`.`travelrecord`");
                    statement.execute("DROP TABLE `db1`.`travelrecord2`;");
                }
        );
    }

    public static void main(String[] args) {
        testShowTableStatus();
        testShowTriggers();
        testLoadData();
        testSetOption();
        testLockUnlock();
        testGrant();
        testChangeDb();
        testCreateDb();
        testDropDb();
        testAlterDb();
        testRepair();
        testReplace();
        testReplaceSelect();
    }

    public static void using(ConsumerIO<Connection> c) {
        try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)) {
            c.accept(connection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    interface ConsumerIO<T> {
        void accept(T t) throws Exception;
    }
}
