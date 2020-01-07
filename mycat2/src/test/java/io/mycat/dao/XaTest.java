package io.mycat.dao;

import org.junit.Assert;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class XaTest {
    public static void main(String[] args) throws Exception {

        try (Connection connection = TestUtil.getMySQLConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("set xa = on");
                //session id:1 action:set xa = 1 exe success
            }
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("select * from db1.company");
                String string = TestUtil.getString(resultSet);
                Assert.assertEquals("(1,Intel,1),(2,IBM,2),(3,Dell,3)", string);
                // proxy target:defaultDs2,sql:select * from db1.company,transaction:false,isolation:REPEATED_READ,master:false,balance:null
            }
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            //session id:1 action: set isolation = REPEATED_READ
            connection.setAutoCommit(true);
            //session id:1 action:set autocommit = 1 exe success
            connection.setAutoCommit(false);
            //session id:1 action:set autocommit = 0 exe success
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("select * from db1.company where id = 4");
                //session id:1 proxy target:defaultDs2,sql:select * from db1.company where id = 1,transaction:false,isolation:REPEATED_READ,master:false,balance:null
                String string = TestUtil.getString(resultSet);
                Assert.assertEquals("", string);
                statement.executeUpdate("INSERT INTO `db1`.`company` (`id`) VALUES ('4');");
                // session id:1 proxy target:defaultDs,sql:INSERT INTO `db1`.`company` (`id`) VALUES ('4');,transaction:true,isolation:REPEATED_READ,master:true,balance:null
                connection.rollback();
                //session id:1 action: rollback from binding session
            }
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("select * from db1.company where id = 4");
                //session id:1 proxy target:defaultDs2,sql:select * from db1.company where id = 1,transaction:false,isolation:REPEATED_READ,master:false,balance:null
                String string = TestUtil.getString(resultSet);
                Assert.assertEquals("", string);
                statement.executeUpdate("INSERT INTO `db1`.`company` (`id`) VALUES ('4');",Statement.RETURN_GENERATED_KEYS);
                ResultSet generatedKeys = statement.getGeneratedKeys();
                long  lastInsertId = (generatedKeys.next() ? generatedKeys.getLong(1) : 0L);
                // session id:1 proxy target:defaultDs,sql:INSERT INTO `db1`.`company` (`id`) VALUES ('4');,transaction:true,isolation:REPEATED_READ,master:true,balance:null
                connection.commit();
                //session id:1 action: commit from binding session
                connection.setAutoCommit(true);
            }
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("delete FROM `db1`.`company` where id = 4");
                ResultSet resultSet = statement.executeQuery("select * from db1.company where id = 4");
                String string = TestUtil.getString(resultSet);
                Assert.assertEquals("", string);
            }
        }
    }
}