package io.mycat.assemble;

import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class AddSequenceTest implements MycatTest {


    @Test
    public void testAddSequence() throws Exception {

        try (Connection connection = getMySQLConnection(DB_MYCAT);) {
            execute(connection,RESET_CONFIG);
            execute(connection, "CREATE DATABASE db1");


            try (Connection mySQLConnection = getMySQLConnection(DB1)) {
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
                    statement.execute("INSERT INTO `db1`.`MYCAT_SEQUENCE` (`name`, `current_value`) VALUES ('db1_travelrecord', '0');");
                }
            }
            addC0(connection);

            execute(connection, "CREATE TABLE db1.`travelrecord` (\n" +
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
            execute(connection, "delete from  db1.travelrecord");
            execute(connection, "INSERT INTO `db1`.`travelrecord` (`user_id`) VALUES ('9999999999');");
            List<Map<String, Object>> maps1 = executeQuery(connection, "select LAST_INSERT_ID()");

            execute(connection, "/*+ mycat:setSequence{\n" +
                    "\t\"name\":\"db1_travelrecord\",\n" +
                    "\t\"clazz\":\"io.mycat.plug.sequence.SequenceMySQLGenerator\",\n" +
                    "} */");
            try (Connection mySQLConnection = getMySQLConnection(DB1)) {
                try (Statement statement = mySQLConnection.createStatement()) {
                    statement.execute("use db1");
                    statement.execute("update `db1`.`MYCAT_SEQUENCE` set `current_value` = '0' where name = 'db1_travelrecord';");
                }
            }
            execute(connection, "delete from  db1.travelrecord");
            execute(connection, "INSERT INTO `db1`.`travelrecord` (`user_id`) VALUES ('9999999999');");
            List<Map<String, Object>> maps2 = executeQuery(connection, "select LAST_INSERT_ID()");
            Assert.assertTrue(maps2.toString().contains("1"));
        }
    }


}
