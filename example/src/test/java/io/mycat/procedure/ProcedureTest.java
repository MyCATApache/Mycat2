package io.mycat.procedure;

import io.mycat.assemble.MycatTest;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class ProcedureTest  implements MycatTest {

    @Test
    @SneakyThrows
    public void baseTest(){
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "CREATE DATABASE db1");
            execute(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
            );
            execute(mycatConnection,"DROP PROCEDURE IF EXISTS mysql.`delete_matches`");
            String s = " CREATE  PROCEDURE mysql.`delete_matches`(\n" +
                    "\tIN p_id INTEGER\n" +
                    ")\n" +
                    "BEGIN\n" +
                    "DELETE FROM db1.`travelrecord` WHERE id = p_id;\n" +
                    "END";
            execute(mycatConnection, s);
            deleteData(mycatConnection,"db1","travelrecord");
            execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('1');");
            CallableStatement callableStatement = mycatConnection.prepareCall(" CALL mysql.delete_matches(1)");
            boolean execute = callableStatement.execute();
            Assert.assertFalse(execute);
            int updateCount = callableStatement.getUpdateCount();
            Assert.assertEquals(1,updateCount);
            System.out.println();
        }
    }

    @Test
    @SneakyThrows
    public void baseTest2(){
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "CREATE DATABASE db1");
            execute(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
            );
            execute(mycatConnection,"DROP PROCEDURE IF EXISTS mysql.`select_matches`");
            String s = " CREATE  PROCEDURE mysql.`select_matches`(\n" +
                    "\tIN p_id INTEGER\n" +
                    ")\n" +
                    "BEGIN\n" +
                    "select * FROM db1.`travelrecord` WHERE id = p_id;\n" +
                    "END";
            execute(mycatConnection, s);
            deleteData(mycatConnection,"db1","travelrecord");
            execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('1');");
            CallableStatement callableStatement = mycatConnection.prepareCall(" CALL mysql.select_matches(1)");
            boolean execute = callableStatement.execute();
            Assert.assertTrue(execute);
            ResultSet resultSet = callableStatement.getResultSet();
            boolean next = resultSet.next();
            Assert.assertTrue(next);
            System.out.println();
        }
    }
}
