package io.mycat.example.assemble;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.example.ExampleObject;
import io.mycat.example.TestUtil;
import io.mycat.example.sharding.ShardingExample;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AssembleExample {

    @Test
    public void testWrapper() throws Exception {
        Connection mySQLConnection = TestUtil.getMySQLConnection();

        // show databases
        executeQuery(mySQLConnection, "show databases");


        // use
        execute(mySQLConnection, "USE `information_schema`;");
        Assert.assertTrue(executeQuery(mySQLConnection, "select database()").toString().contains("information_schema"));
        execute(mySQLConnection, "USE `mysql`;");

        // database();
        Assert.assertTrue(executeQuery(mySQLConnection, "select database()").toString().contains("mysql"));

        // VERSION()
        Assert.assertTrue(executeQuery(mySQLConnection, "select VERSION()").toString().contains("8.19"));

        // LAST_INSERT_ID()
        executeQuery(mySQLConnection, "select CONNECTION_ID()");

        // CURRENT_USER()
        executeQuery(mySQLConnection, "select CURRENT_USER()");

        // SYSTEM_USER()
        executeQuery(mySQLConnection, "select SYSTEM_USER()");

        // SESSION_USER()
        executeQuery(mySQLConnection, "select SESSION_USER()");
        System.out.println();
    }

    private void execute(Connection mySQLConnection, String sql) throws SQLException {
        JdbcUtils.execute(mySQLConnection, sql);
    }


    public static List<Map<String, Object>> executeQuery(Connection mySQLConnection, String sql) throws SQLException {
        return JdbcUtils.executeQuery(mySQLConnection, sql, Collections.emptyList());
    }

}
