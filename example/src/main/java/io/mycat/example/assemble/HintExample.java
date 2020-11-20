package io.mycat.example.assemble;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class HintExample {

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
    public void testShowBufferUsage() throws SQLException {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            try (Statement statement = mycatConnection.createStatement()) {
                ResultSet resultSet = statement.executeQuery(
                        "/*+ mycat:showBufferUsage{}*/");
                resultSet.next();
            }
        }
    }

    @Test
    public void testShowUsers() throws SQLException {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            List<Map<String, Object>> maps = AssembleExample.executeQuery(mycatConnection,
                    "/*+ mycat:showUsers{}*/");
            Assert.assertTrue(!maps.isEmpty());
        }
    }

    @Test
    public void testShowSchemas() throws SQLException {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            Assert.assertTrue(AssembleExample
                    .executeQuery(mycatConnection,
                            "/*+ mycat:showSchemas{}*/").size() > 2);
            Assert.assertTrue(AssembleExample
                    .executeQuery(mycatConnection,
                            "/*+ mycat:showSchemas{\"schemaName\":\"mysql\"}*/").size() == 1);
        }
    }

    @Test
    public void testShowSchedules() throws SQLException {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            AssembleExample
                    .executeQuery(mycatConnection,
                            "/*+ mycat:showSchedules{}*/");
        }
    }

    @Test
    public void testShowClusters() throws SQLException {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            Assert.assertTrue(AssembleExample
                    .executeQuery(mycatConnection,
                            "/*+ mycat:showClusters{}*/").size() > 0);
        }
    }

    @Test
    public void testShowDatasources() throws SQLException {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            Assert.assertTrue(AssembleExample
                    .executeQuery(mycatConnection,
                            "/*+ mycat:showDatasources{}*/").size() > 0);
        }
    }

    @Test
    public void testShowHeartbeats() throws SQLException {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            Assert.assertTrue(AssembleExample
                    .executeQuery(mycatConnection,
                            "/*+ mycat:showHeartbeats{}*/").size() > 0);
        }
    }

    @Test
    public void testShowHeartbeatStatus() throws SQLException {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            Assert.assertTrue(AssembleExample
                    .executeQuery(mycatConnection,
                            "/*+ mycat:showHeartbeatStatus{}*/").size() > 0);
        }
    }

    @Test
    public void testShowInstances() throws SQLException {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            Assert.assertTrue(AssembleExample
                    .executeQuery(mycatConnection,
                            "/*+ mycat:showInstances{}*/").size() > 0);
        }
    }

    @Test
    public void testShowReactors() throws SQLException {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            Assert.assertTrue(AssembleExample
                    .executeQuery(mycatConnection,
                            "/*+ mycat:showReactors{}*/").size() > 0);
        }
    }

    @Test
    public void testShowThreadPools() throws SQLException {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            Assert.assertTrue(AssembleExample
                    .executeQuery(mycatConnection,
                            "/*+ mycat:showThreadPools{}*/").size() > 0);
        }
    }

    @Test
    public void testShowTables() throws SQLException {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            Assert.assertTrue(AssembleExample
                    .executeQuery(mycatConnection,
                            "/*+ mycat:showTables{}*/").size() > 2);
            Assert.assertTrue(!AssembleExample
                    .executeQuery(mycatConnection,
                            "/*+ mycat:showTables{\"schemaName\":\"mysql\"}*/").isEmpty());
        }
    }

    @Test
    public void testShowConnections() throws SQLException {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            AssembleExample
                    .executeQuery(mycatConnection,
                            "/*+ mycat:showConnections{}*/");
        }
    }


}
