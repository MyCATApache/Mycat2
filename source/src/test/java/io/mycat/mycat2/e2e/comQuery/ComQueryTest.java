package io.mycat.mycat2.e2e.comQuery;

import io.mycat.mycat2.e2e.BaseSQLExeTest;
import io.mycat.mycat2.e2e.BaseSQLTest;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

/**
 * Created by linxiaofang on 2018/11/5.
 * create database db1;
 * create table travelrecord (id bigint not null primary key,user_id varchar(100),traveldate DATE, fee decimal,days int);
 */
public class ComQueryTest extends BaseSQLTest {
    final static String REPL_MASTER_HOST = "192.168.1.6";
    final static int REPL_MASTER_PORT = 3306;
    final static String REPL_MASTER_USER = "repl";
    final static String REPL_MASTER_PASSWORD = "";
    final static String REPL_MASTER_LOG_FILE = "mysql-bin.000001";
    final static int REPL_MASTER_LOG_POS = 7849;

    @Test
    public void testShowTableStatus() {
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

    @Test
    public void testShowTriggers() {
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
     * 如果报错: The MySQL server is running with the --secure-file-priv option so it cannot execute this statement
     * 需要修改my.cnf, 增加 secure-file-priv="" 后重启
     */
    @Test
    public void testLoadData() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("TRUNCATE TABLE `db1`.`travelrecord`;");
                    statement.execute("LOAD DATA INFILE '/tmp/loaddata.txt' INTO TABLE `db1`.`travelrecord` FIELDS TERMINATED BY ',';");
                }
        );
    }

    @Test
    public void testSetOption() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.executeQuery("SET @Name=23");
                }
        );
    }

    @Test
    public void testLockUnlock() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.executeQuery("LOCK TABLES `db1`.`travelrecord` READ;");
                    statement.executeQuery("SELECT COUNT(*) FROM `db1`.`travelrecord`;");
                    statement.executeQuery("UNLOCK TABLES;");
                }
        );
    }

    @Test
    public void testGrantRevoke() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("CREATE USER 'jeffreyJF'@'localhost' IDENTIFIED BY 'What?2018';");
                    statement.execute("FLUSH PRIVILEGES;");
                    statement.execute("RENAME USER 'jeffreyJF'@'localhost' TO 'jeffrey'@'%';");
                    statement.execute("FLUSH PRIVILEGES;");
                    statement.execute("GRANT ALL ON db1.* TO 'jeffrey'@'%';");
                    statement.execute("REVOKE ALL ON db1.* FROM 'jeffrey'@'%';");
                    statement.execute("SHOW PRIVILEGES;");
                    statement.execute("DROP USER 'jeffrey'@'%';");
                    statement.execute("FLUSH PRIVILEGES;");
                }
        );
    }

    @Test
    public void testChangeDb() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("USE `db1`;");
                }
        );
    }

    @Test
    public void testCreateDb() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("CREATE DATABASE IF NOT EXISTS `menagerie`;");
                }
        );
    }

    @Test
    public void testDropDb() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("DROP DATABASE IF EXISTS `menagerie`;");
                }
        );
    }

    @Test
    public void testAlterDb() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("ALTER TABLE `db1`.`travelrecord` ADD i INT FIRST;");
                    statement.execute("ALTER TABLE `db1`.`travelrecord` DROP i;");
                }
        );
    }

    @Test
    public void testRepair() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("REPAIR TABLE `db1`.`travelrecord`;");
                }
        );
    }

    @Test
    public void testReplace() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("REPLACE INTO `db1`.`travelrecord` (`id`, `user_id`, `traveldate`, `fee`, `days`) VALUES (3, '2', '2018-11-02', '2', '2') ;");
                }
        );
    }

    @Test
    public void testReplaceSelect() {
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

    @Test
    public void testCreateDropFunction() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("DROP FUNCTION IF EXISTS `hello`");
                    statement.execute("CREATE FUNCTION `hello` (s CHAR(20))\n" +
                            " RETURNS CHAR(50) DETERMINISTIC\n" +
                            " RETURN CONCAT('Hello, ',s,'!');");
                }
        );
    }

    @Test
    public void testOptimize() {
        using(c -> {
                    Statement statement = c.createStatement();
                    Assert.assertTrue(statement.execute("OPTIMIZE TABLE `db1`.`travelrecord`;"));
                }
        );
    }

    @Test
    public void testCheck() {
        using(c -> {
                    Statement statement = c.createStatement();
                    Assert.assertTrue(statement.execute("CHECK TABLE `db1`.`travelrecord`;"));
                }
        );
    }

    @Test
    public void testCacheIndex() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("SET GLOBAL hot_cache.key_buffer_size=128*1024;");
                    statement.execute("CACHE INDEX `db1`.`travelrecord` IN hot_cache;");
                    statement.execute("LOAD INDEX INTO CACHE `db1`.`travelrecord` IGNORE LEAVES;");
                }
        );
    }

    @Test
    public void testFlush() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("FLUSH PRIVILEGES;");
                }
        );
    }

    /*
     * 执行此方法前先手动执行select sleep(100),然后执行show processlist,找出对应的processId再执行kill命令
     */
//    @Test
    public void testKill() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("KILL 333");
                }
        );
    }

    @Test
    public void testAnalyze() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("ANALYZE TABLE `db1`.`travelrecord`;");
                }
        );
    }

    @Test
    public void testBegin() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("BEGIN;");
                    statement.execute("DELETE FROM `db1`.`travelrecord` WHERE `id` = 3;");
                    statement.execute("ROLLBACK;");
                }
        );
    }

    @Test
    public void testRollback() {
        using(c -> {
                    Statement statement = c.createStatement();
                    c.setAutoCommit(false);
                    statement.executeUpdate("DELETE FROM `db1`.`travelrecord` WHERE `id` = 3;");
                    c.rollback();
                }
        );
    }

    @Test
    public void testRollbackToSavePoint() {
        using(c -> {
                    Statement statement = c.createStatement();
                    c.setAutoCommit(false);
                    statement.executeUpdate("REPLACE INTO `db1`.`travelrecord` (`id`, `user_id`, `traveldate`, `fee`, `days`) VALUES (4, '2', '2018-11-02', '2', '2') ;");

                    Savepoint sp = c.setSavepoint("after place");
                    statement.executeUpdate("DELETE FROM `db1`.`travelrecord` WHERE `id` = 3;");
                    c.rollback(sp);
                    c.commit();
                }
        );
    }

    @Test
    public void testReleaseSavePoint() {
        using(c -> {
                    Statement statement = c.createStatement();
                    c.setAutoCommit(false);
                    statement.executeUpdate("REPLACE INTO `db1`.`travelrecord` (`id`, `user_id`, `traveldate`, `fee`, `days`) VALUES (4, '2', '2018-11-02', '2', '2') ;");

                    Savepoint sp = c.setSavepoint("after place");
                    c.releaseSavepoint(sp);
                    c.rollback();
                }
        );
    }

    /*
     * 执行testSlave,需要连接到Slave数据库
     */
    @Test
    public void testSlave() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("STOP SLAVE;");
                    statement.execute("CHANGE MASTER to MASTER_HOST='" + REPL_MASTER_HOST + "',MASTER_PORT=" + REPL_MASTER_PORT + ",MASTER_USER='" + REPL_MASTER_USER +
                            "',MASTER_PASSWORD='" + REPL_MASTER_PASSWORD + "',MASTER_LOG_FILE='" + REPL_MASTER_LOG_FILE + "', MASTER_LOG_POS=" + REPL_MASTER_LOG_POS + ";");
                    statement.execute("CHANGE REPLICATION FILTER REPLICATE_DO_DB = (db1);");
                    statement.execute("START SLAVE;");
                }
        );
    }

    /*
     * 必须开启group replication,否则会报异常:java.sql.SQLException: The server is not configured properly to be an active member of the group.
     */
//    @Test
    public void testGroupReplication() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("START GROUP_REPLICATION;");
                    statement.execute("STOP GROUP_REPLICATION;");
                }
        );
    }

    /*
     * 必须开启binlog才能执行,否则会报异常: java.sql.SQLException: You are not using binary logging
     */
    @Test
    public void testBinlog() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("SHOW BINARY LOGS;");
                    statement.execute("SHOW MASTER LOGS;");
                    statement.execute("SHOW BINLOG EVENTS;");
                    statement.execute("PURGE BINARY LOGS BEFORE '2008-04-02 22:46:26';");
                }
        );
    }

    /*
     * 空查询会报异常: jjava.sql.SQLException: Can not issue empty query.
     */
//    @Test
    public void testEmptyQuery() {
        using(c -> {
                Statement statement = c.createStatement();
                statement.executeQuery("");
            }
        );
    }

    @Test
    public void testShowSlaveHosts() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("SHOW SLAVE HOSTS;");
                }
        );
    }

    @Test
    public void testReset() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("RESET QUERY CACHE;");
                }
        );
    }

    @Test
    public void testRenameTable() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("RENAME TABLE `db1`.`travelrecord` TO `db1`.`travelrecord2`;");
                    statement.execute("RENAME TABLE `db1`.`travelrecord2` TO `db1`.`travelrecord`;");
                }
        );
    }

    @Test
    public void testShowOpenTables() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("SHOW OPEN TABLES;");
                }
        );
    }

    /*
     * 执行报错:com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException: You have an error in your SQL syntax;
     */
//    @Test
    public void testShowHelp() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("HELP;");
                }
        );
    }

    @Test
    public void testShowStorageEngines() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("SHOW STORAGE ENGINES;");
                }
        );
    }

    @Test
    public void testShowWarningsErrors() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("SHOW WARNINGS;");
                    statement.execute("SHOW ERRORS;");
                }
        );
    }

    @Test
    public void testDo() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("DO SLEEP(1);");
                }
        );
    }

    @Test
    public void testHandler() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("HANDLER `db1`.`travelrecord` OPEN;");
                    statement.execute("HANDLER travelrecord READ FIRST;");
                    statement.execute("HANDLER travelrecord CLOSE;");
                }
        );
    }

    @Test
    public void testDeleteMulti() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("CREATE TABLE IF NOT EXISTS `db1`.`t1` (`id` int(11) NOT NULL, `count` int(11) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;");
                    statement.execute("CREATE TABLE IF NOT EXISTS `db1`.`t2` (`id` int(11) NOT NULL, `count` int(11) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;");
                    statement.executeUpdate("INSERT INTO `db1`.`t1` (`id`, `count`) VALUES (1, 1) ;");
                    statement.executeUpdate("INSERT INTO `db1`.`t2` (`id`, `count`) VALUES (1,2 ) ;");
                    statement.execute("DELETE `db1`.`t1`, `db1`.`t2` FROM `db1`.`t1` INNER JOIN `db1`.`t2` WHERE t1.id=t2.id;");
            }
        );
    }

    @Test
    public void testUpdateMulti() {
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("CREATE TABLE IF NOT EXISTS `db1`.`t1` (`id` int(11) NOT NULL, `count` int(11) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;");
                    statement.execute("CREATE TABLE IF NOT EXISTS `db1`.`t2` (`id` int(11) NOT NULL, `count` int(11) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;");
                    statement.executeUpdate("INSERT INTO `db1`.`t1` (`id`, `count`) VALUES (1, 1) ;");
                    statement.executeUpdate("INSERT INTO `db1`.`t2` (`id`, `count`) VALUES (1, 2) ;");
                    statement.execute("UPDATE `db1`.`t1`, `db1`.`t2` SET `db1`.`t1`.`count`=`db1`.`t1`.`count`+1, `db1`.`t2`.`count`=`db1`.`t2`.`count`-1 WHERE t1.id=t2.id;");
                }
        );
    }
}
