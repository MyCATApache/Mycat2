package io.mycat.dao;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class ReadWriteSeparationTest {
    private static final Logger logger = LoggerFactory.getLogger(ReadWriteSeparationTest.class);

    /**
     * 移除元数据db1.company配置
     * 添加以下配置
     *
     *         {
     *              tables:[ 'db1.company'],
     *              sqls: [
     *              {sql: 'select {any}',command: execute ,tags: {targets: repli,executeType: QUERY ,needTransaction: true}},
     *              {sql: 'select {any} for update',command: execute ,tags: {executeType: QUERY_MASTER ,targets: repli,needTransaction: true}},
     *              {sql: 'insert {any}',command: execute, tags: {executeType: UPDATE ,targets: repli,needTransaction: true,}},
     *              {sql: 'delete {any}',command: execute, tags: {executeType: UPDATE ,targets: repli,needTransaction: true,}}
     *              ],
     *            },
     *
     * @param args
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {
        try(Connection mySQLConnection = TestUtil.getMySQLConnection()){
            try(Statement statement = mySQLConnection.createStatement()){
                statement.execute("set xa = on");
                statement.execute("delete db1.travelrecord");
                statement.execute("INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('1')");
                statement.execute("delete FROM db1.company");
                statement.execute("INSERT INTO `db1`.`company` (`id`, `companyname`, `addressid`) VALUES ('1','Intel','1'),('2','IBM','2'),('3','Dell','3')");
            }
        }
        List<String> initList = Arrays.asList("set xa = off");
        test(TestUtil.getMySQLConnection(), initList);
        test(TestUtil.getMariaDBConnection(), initList);

        List<String> initList2 = Arrays.asList("set xa = on");
        test(TestUtil.getMySQLConnection(), initList2);
        test(TestUtil.getMariaDBConnection(), initList2);
    }

    private static void test(Connection mySQLConnection, List<String> initList) throws SQLException {
        //action:set xa = 0 exe success
        try (Connection connection = mySQLConnection) {
            try (Statement statement = connection.createStatement()) {
                for (String s : initList) statement.execute(s);
                //action:set xa = 0 exe success
            }
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("select * from db1.company");
                String string = TestUtil.getString(resultSet);
                Assert.assertEquals("(1,Intel,1)(2,IBM,2)(3,Dell,3)", string);
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
                ResultSet resultSet = statement.executeQuery("select * from db1.company where id > 3");
                //session id:1 proxy target:defaultDs2,sql:select * from db1.company where id = 1,transaction:false,isolation:REPEATED_READ,master:false,balance:null
                String string = TestUtil.getString(resultSet);
                Assert.assertEquals("", string);
                statement.executeUpdate("INSERT INTO `db1`.`company` () VALUES ();", Statement.RETURN_GENERATED_KEYS);
                ResultSet generatedKeys = statement.getGeneratedKeys();
                long lastInsertId = (generatedKeys.next() ? generatedKeys.getLong(1) : 0L);
                Assert.assertTrue(lastInsertId > 0);
                // session id:1 proxy target:defaultDs,sql:INSERT INTO `db1`.`company` (`id`) VALUES ('4');,transaction:true,isolation:REPEATED_READ,master:true,balance:null
                connection.commit();
                //session id:1 action: commit from binding session
                connection.setAutoCommit(true);
            }
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("delete FROM `db1`.`company` where id > 3");
                ResultSet resultSet = statement.executeQuery("select * from db1.company where id > 3");
                String string = TestUtil.getString(resultSet);
                Assert.assertEquals("", string);
            }
        }
    }

}