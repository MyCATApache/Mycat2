package io.mycat.example.assemble;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.parser.SQLParserUtils;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class TestAddSequence {
    Connection getMySQLConnection(int port) throws SQLException {
        String username = "root";
        String password = "123456";
        String url = "jdbc:mysql://127.0.0.1:" + port;
        MysqlDataSource mysqlDataSource = new MysqlDataSource();
        mysqlDataSource.setUrl(url);
        mysqlDataSource.setUser(username);
        mysqlDataSource.setPassword(password);
        mysqlDataSource.setServerTimezone("UTC");
        return mysqlDataSource.getConnection();
    }

    @Test
    public void testAddSequence() throws SQLException {

        try (Connection connection = getMySQLConnection(8066)) {
            AssembleExample.execute(connection, "CREATE DATABASE db1");


            try (Connection mySQLConnection = getMySQLConnection(3306)) {
                try (Statement statement = mySQLConnection.createStatement()) {
                    statement.execute("use db1");
                    statement.execute("DROP TABLE IF EXISTS MYCAT_SEQUENCE;");
                    statement.execute("CREATE TABLE MYCAT_SEQUENCE (  NAME VARCHAR(64) NOT NULL,  current_value BIGINT(20) NOT NULL,  increment INT NOT NULL DEFAULT 1, PRIMARY KEY (NAME) ) ENGINE=INNODB;\n");
                    statement.execute("DROP FUNCTION IF EXISTS `mycat_seq_nextval`;");
                    statement.execute("CREATE FUNCTION `mycat_seq_nextval`(seq_name VARCHAR(64)) RETURNS VARCHAR(64) CHARSET latin1\n" +
                            "    DETERMINISTIC\n" +
                            "BEGIN\n" +
                            "    DECLARE retval VARCHAR(64);\n" +
                            "    DECLARE val BIGINT;\n" +
                            "    DECLARE inc INT;\n" +
                            "    DECLARE seq_lock INT;\n" +
                            "    SET val = -1;\n" +
                            "    SET inc = 0;\n" +
                            "    SET seq_lock = -1;\n" +
                            "    SELECT GET_LOCK(seq_name, 15) INTO seq_lock;\n" +
                            "    IF seq_lock = 1 THEN\n" +
                            "      SELECT current_value + increment, increment INTO val, inc FROM MYCAT_SEQUENCE WHERE NAME = seq_name FOR UPDATE;\n" +
                            "      IF val != -1 THEN\n" +
                            "          UPDATE MYCAT_SEQUENCE SET current_value = val WHERE NAME = seq_name;\n" +
                            "      END IF;\n" +
                            "      SELECT RELEASE_LOCK(seq_name) INTO seq_lock;\n" +
                            "    END IF;\n" +
                            "    SELECT CONCAT(CAST((val - inc + 1) AS CHAR),\",\",CAST(inc AS CHAR)) INTO retval;\n" +
                            "    RETURN retval;\n" +
                            "END");
                    statement.execute("INSERT INTO `db1`.`mycat_sequence` (`name`, `current_value`) VALUES ('db1_travelrecord', '0');");
                }
            }


            AssembleExample.execute(connection, "CREATE TABLE db1.`travelrecord` (\n" +
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
            AssembleExample.execute(connection, "delete from  db1.travelrecord");
            AssembleExample.execute(connection, "INSERT INTO `db1`.`travelrecord` (`user_id`) VALUES ('9999999999');");
            List<Map<String, Object>> maps1 = AssembleExample.executeQuery(connection, "select last_insert_id()");

            AssembleExample.execute(connection, "/*+ mycat:setSequence{\n" +
                    "\t\"name\":\"db1_travelrecord\",\n" +
                    "\t\"clazz\":\"io.mycat.plug.sequence.SequenceMySQLGenerator\",\n" +
                    "} */");
            try (Connection mySQLConnection = getMySQLConnection(3306)) {
                try (Statement statement = mySQLConnection.createStatement()) {
                    statement.execute("use db1");
                    statement.execute("update `db1`.`mycat_sequence` set `current_value` = '0' where name = 'db1_travelrecord';");
                }
            }
            AssembleExample.execute(connection, "delete from  db1.travelrecord");
            AssembleExample.execute(connection, "INSERT INTO `db1`.`travelrecord` (`user_id`) VALUES ('9999999999');");
            List<Map<String, Object>> maps2 = AssembleExample.executeQuery(connection, "select last_insert_id()");
            Assert.assertTrue(maps2.toString().contains("1"));
        }
    }

    static final String dbseq = "DROP TABLE IF EXISTS MYCAT_SEQUENCE;\nCREATE TABLE MYCAT_SEQUENCE (  name VARCHAR(64) NOT NULL,  current_value BIGINT(20) NOT NULL,  increment INT NOT NULL DEFAULT 1, PRIMARY KEY (name) ) ENGINE=InnoDB;\n\n-- ----------------------------\n-- Function structure for `mycat_seq_currval`\n-- ----------------------------\nDROP FUNCTION IF EXISTS `mycat_seq_currval`;\nDELIMITER ;;\nCREATE FUNCTION `mycat_seq_currval`(seq_name VARCHAR(64)) RETURNS varchar(64) CHARSET latin1\n    DETERMINISTIC\nBEGIN\n    DECLARE retval VARCHAR(64);\n    SET retval=\"-1,0\";\n    SELECT concat(CAST(current_value AS CHAR),\",\",CAST(increment AS CHAR) ) INTO retval FROM MYCAT_SEQUENCE  WHERE name = seq_name;\n    RETURN retval ;\nEND\n;;\nDELIMITER ;\n\n-- ----------------------------\n-- Function structure for `mycat_seq_nextval`\n-- ----------------------------\nDROP FUNCTION IF EXISTS `mycat_seq_nextval`;\nDELIMITER ;;\nCREATE FUNCTION `mycat_seq_nextval`(seq_name VARCHAR(64)) RETURNS varchar(64) CHARSET latin1\n    DETERMINISTIC\nBEGIN\n    DECLARE retval VARCHAR(64);\n    DECLARE val BIGINT;\n    DECLARE inc INT;\n    DECLARE seq_lock INT;\n    set val = -1;\n    set inc = 0;\n    SET seq_lock = -1;\n    SELECT GET_LOCK(seq_name, 15) into seq_lock;\n    if seq_lock = 1 then\n      SELECT current_value + increment, increment INTO val, inc FROM MYCAT_SEQUENCE WHERE name = seq_name for update;\n      if val != -1 then\n          UPDATE MYCAT_SEQUENCE SET current_value = val WHERE name = seq_name;\n      end if;\n      SELECT RELEASE_LOCK(seq_name) into seq_lock;\n    end if;\n    SELECT concat(CAST((val - inc + 1) as CHAR),\",\",CAST(inc as CHAR)) INTO retval;\n    RETURN retval;\nEND\n;;\nDELIMITER ;\n\n-- ----------------------------\n-- Function structure for `mycat_seq_setvals`\n-- ----------------------------\nDROP FUNCTION IF EXISTS `mycat_seq_nextvals`;\nDELIMITER ;;\nCREATE FUNCTION `mycat_seq_nextvals`(seq_name VARCHAR(64), count INT) RETURNS VARCHAR(64) CHARSET latin1\n    DETERMINISTIC\nBEGIN\n    DECLARE retval VARCHAR(64);\n    DECLARE val BIGINT;\n    DECLARE seq_lock INT;\n    SET val = -1;\n    SET seq_lock = -1;\n    SELECT GET_LOCK(seq_name, 15) into seq_lock;\n    if seq_lock = 1 then\n        SELECT current_value + count INTO val FROM MYCAT_SEQUENCE WHERE name = seq_name for update;\n        IF val != -1 THEN\n            UPDATE MYCAT_SEQUENCE SET current_value = val WHERE name = seq_name;\n        END IF;\n        SELECT RELEASE_LOCK(seq_name) into seq_lock;\n    end if;\n    SELECT CONCAT(CAST((val - count + 1) as CHAR), \",\", CAST(val as CHAR)) INTO retval;\n    RETURN retval;\nEND\n;;\nDELIMITER ;\n\n-- ----------------------------\n-- Function structure for `mycat_seq_setval`\n-- ----------------------------\nDROP FUNCTION IF EXISTS `mycat_seq_setval`;\nDELIMITER ;;\nCREATE FUNCTION `mycat_seq_setval`(seq_name VARCHAR(64), value BIGINT) RETURNS varchar(64) CHARSET latin1\n    DETERMINISTIC\nBEGIN\n    DECLARE retval VARCHAR(64);\n    DECLARE inc INT;\n    SET inc = 0;\n    SELECT increment INTO inc FROM MYCAT_SEQUENCE WHERE name = seq_name;\n    UPDATE MYCAT_SEQUENCE SET current_value = value WHERE name = seq_name;\n    SELECT concat(CAST(value as CHAR),\",\",CAST(inc as CHAR)) INTO retval;\n    RETURN retval;\nEND\n;;\nDELIMITER ;\n\nINSERT INTO MYCAT_SEQUENCE VALUES ('GLOBAL', 1, 1);";
}
