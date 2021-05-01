package io.mycat.sql;

import io.mycat.assemble.MycatTest;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.Statement;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class MultiStatementTest implements MycatTest {
    @Test
    public void testSelect() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(
                "jdbc:mysql://localhost:8066/mysql?username=root&password=123456&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true"
                +"&allowMultiQueries=true"
        )) {
            Statement statement = mycatConnection.createStatement();
            statement.execute("select 1;select 1;");
        }
    }
    @Test
    public void testSelectOk() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(
                "jdbc:mysql://localhost:8066/mysql?username=root&password=123456&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true"
                        +"&allowMultiQueries=true"
        )) {
            Statement statement = mycatConnection.createStatement();
            statement.execute("select 1;set autocommit = 1;");
        }
    }
    @Test
    public void testOkSelect() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(
                "jdbc:mysql://localhost:8066/mysql?username=root&password=123456&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true"
                        +"&allowMultiQueries=true"
        )) {
            Statement statement = mycatConnection.createStatement();
            statement.execute("set autocommit = 1;select 1");
        }
    }
    @Test(expected= Exception.class)
    public void testSelectError() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(
                "jdbc:mysql://localhost:8066/mysql?username=root&password=123456&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true"
                        +"&allowMultiQueries=true"
        )) {
            Statement statement = mycatConnection.createStatement();

            statement.execute("select 1;select 1/0");
        }
    }
    @Test(expected= Exception.class)
    public void testOkError() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(
                "jdbc:mysql://localhost:8066/mysql?username=root&password=123456&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true"
                        +"&allowMultiQueries=true"
        )) {
            Statement statement = mycatConnection.createStatement();
            statement.execute("select 1;begin;begin;");
        }
    }
}
