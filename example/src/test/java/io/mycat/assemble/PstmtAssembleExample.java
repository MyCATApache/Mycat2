package io.mycat.assemble;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class PstmtAssembleExample extends AssembleExample {

    @Override
    Connection getMySQLConnection(int port) throws SQLException {
        String username = "root";
        String password = "123456";
        String url = "jdbc:mysql://127.0.0.1:" +
                port ;
        MysqlDataSource mysqlDataSource = new MysqlDataSource();
        mysqlDataSource.setUrl(url);
        mysqlDataSource.setUser(username);
        mysqlDataSource.setPassword(password);
        mysqlDataSource.setUseServerPrepStmts(true);
        mysqlDataSource.setServerTimezone("UTC");
        return mysqlDataSource.getConnection();
    }
    @Test
    @Override
    public void testTranscationFail2() throws Exception {
        super.testTranscationFail2();
    }
    @Test
    @Override
    public void testTranscationFail() throws Exception {
        super.testTranscationFail();
    }
    @Test
    @Override
    public void testBase() throws Exception {
        super.testBase();
    }
    @Test
    @Override
    public void testInfoFunction() throws SQLException {
        super.testInfoFunction();
    }
}
