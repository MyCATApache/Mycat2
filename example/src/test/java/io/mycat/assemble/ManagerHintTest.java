package io.mycat.assemble;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class ManagerHintTest implements MycatTest {

    @Test
    public void testShowBufferUsage() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            execute(mycatConnection,RESET_CONFIG);
            try (Statement statement = mycatConnection.createStatement()) {

                ResultSet resultSet = statement.executeQuery(
                        "/*+ mycat:showBufferUsage{}*/");
                resultSet.next();
            }
        }
    }

    @Test
    public void testShowUsers() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            execute(mycatConnection,RESET_CONFIG);
            List<Map<String, Object>> maps = executeQuery(mycatConnection,
                    "/*+ mycat:showUsers{}*/");
            Assert.assertTrue(!maps.isEmpty());
        }
    }

    @Test
    public void testShowSchemas() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            execute(mycatConnection,RESET_CONFIG);
            Assert.assertTrue(
                    executeQuery(mycatConnection,
                            "/*+ mycat:showSchemas{}*/").size() > 2);
            Assert.assertTrue(executeQuery(mycatConnection,
                            "/*+ mycat:showSchemas{\"schemaName\":\"mysql\"}*/").size() == 1);
        }
    }

    @Test
    public void testShowSchedules() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
                    executeQuery(mycatConnection,
                            "/*+ mycat:showSchedules{}*/");
        }
    }

    @Test
    public void testShowClusters() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            Assert.assertTrue(executeQuery(mycatConnection,
                            "/*+ mycat:showClusters{}*/").size() > 0);
        }
    }

    @Test
    public void testShowDatasources() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            Assert.assertTrue(executeQuery(mycatConnection,
                            "/*+ mycat:showDatasources{}*/").size() > 0);
        }
    }

    @Test
    public void testShowHeartbeats() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            Assert.assertTrue(executeQuery(mycatConnection,
                            "/*+ mycat:showHeartbeats{}*/").size() > 0);
        }
    }

    @Test
    public void testShowHeartbeatStatus() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            Assert.assertTrue(executeQuery(mycatConnection,
                            "/*+ mycat:showHeartbeatStatus{}*/").size() > 0);
        }
    }

    @Test
    public void testShowInstances() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            Assert.assertTrue(executeQuery(mycatConnection,
                            "/*+ mycat:showInstances{}*/").size() > 0);
        }
    }

    @Test
    public void testShowReactors() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            Assert.assertTrue(executeQuery(mycatConnection,
                            "/*+ mycat:showReactors{}*/").size() > 0);
        }
    }

    @Test
    public void testShowThreadPools() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            Assert.assertTrue(executeQuery(mycatConnection,
                            "/*+ mycat:showThreadPools{}*/").size() > 0);
        }
    }

    @Test
    public void testShowTables() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            Assert.assertTrue(executeQuery(mycatConnection,
                            "/*+ mycat:showTables{}*/").size() > 2);
            Assert.assertTrue(!executeQuery(mycatConnection,
                            "/*+ mycat:showTables{\"schemaName\":\"mysql\"}*/").isEmpty());
        }
    }

    @Test
    public void testShowConnections() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(8066)) {
            executeQuery(mycatConnection,
                            "/*+ mycat:showConnections{}*/");
        }
    }


}
