package io.mycat.example.assemble;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.config.ClusterConfig;
import io.mycat.config.DatasourceConfig;
import io.mycat.example.ExampleObject;
import io.mycat.example.TestUtil;
import io.mycat.example.sharding.ShardingExample;
import io.mycat.util.JsonUtil;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;

public class AssembleExample {

    @Test
    public void testWrapper() throws Exception {
        Connection mycatConnection = TestUtil.getMySQLConnection(8066);


        Connection mysql3306 = TestUtil.getMySQLConnection(3306);
        Connection mysql3307 = TestUtil.getMySQLConnection(3307);


        List<Map<String, Object>> maps = executeQuery(mycatConnection,
                "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'db1' UNION SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'xxx' UNION SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = 'db1' ");

        // show databases
        executeQuery(mycatConnection, "show databases");


        // use
        execute(mycatConnection, "USE `information_schema`;");
        Assert.assertTrue(executeQuery(mycatConnection, "select database()").toString().contains("information_schema"));
        execute(mycatConnection, "USE `mysql`;");

        // database();
        Assert.assertTrue(executeQuery(mycatConnection, "select database()").toString().contains("mysql"));

        // VERSION()
        Assert.assertTrue(executeQuery(mycatConnection, "select VERSION()").toString().contains("8.19"));

        // LAST_INSERT_ID()
        executeQuery(mycatConnection, "select CONNECTION_ID()");

        // CURRENT_USER()
        executeQuery(mycatConnection, "select CURRENT_USER()");

        // SYSTEM_USER()
        executeQuery(mycatConnection, "select SYSTEM_USER()");

        // SESSION_USER()
        executeQuery(mycatConnection, "select SESSION_USER()");

        executeQuery(mycatConnection, "select SESSION_USER()");


        execute(mycatConnection, "DROP DATABASE db1");
        Assert.assertFalse(executeQuery(mycatConnection, "show databases").toString().contains("db1"));

        execute(mycatConnection, "CREATE DATABASE db1");
        Assert.assertTrue(executeQuery(mycatConnection, "show databases").toString().contains("db1"));

        execute(mycatConnection, "drop table db1.travelrecord");

        Assert.assertFalse(
                executeQuery(mycatConnection,
                        "SHOW FULL TABLES FROM `db1` WHERE table_type = 'BASE TABLE';").toString().contains("travelrecord")
        );

        execute(mycatConnection, "USE `db1`;");
        execute(mycatConnection, "CREATE TABLE `travelrecord` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `user_id` varchar(100) DEFAULT NULL,\n" +
                "  `traveldate` date DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int DEFAULT NULL,\n" +
                "  `blob` longblob,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `id` (`id`)\n" +
                ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");

        Assert.assertTrue(
                executeQuery(mycatConnection,
                        "SHOW FULL TABLES FROM `db1` WHERE table_type = 'BASE TABLE';").toString().contains("travelrecord")
        );

        Assert.assertTrue(
                executeQuery(mycatConnection, "select * from travelrecord limit 1").isEmpty()
        );

        execute(mycatConnection,
                "insert  into `travelrecord`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`) values (12,'999',NULL,NULL,NULL,NULL);"
        );

        Assert.assertTrue(
                executeQuery(mycatConnection, "select LAST_INSERT_ID()").toString().contains("12")
        );
        execute(mycatConnection, "\n" +
                "insert  into `travelrecord`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`) values (1,'999',NULL,NULL,NULL,NULL),(2,NULL,NULL,NULL,NULL,NULL),(6666,NULL,NULL,NULL,NULL,NULL),(999999999,'999',NULL,NULL,NULL,NULL);\n");

        Assert.assertTrue(
                executeQuery(mycatConnection, "select LAST_INSERT_ID()").toString().contains("999999999")
        );

        Assert.assertFalse(
                executeQuery(mycatConnection, "select * from travelrecord limit 1").isEmpty()
        );

        execute(mycatConnection, "drop table db1.travelrecord");

        execute(mycatConnection, "CREATE TABLE `travelrecord` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `user_id` varchar(100) DEFAULT NULL,\n" +
                "  `traveldate` date DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int DEFAULT NULL,\n" +
                "  `blob` longblob,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `id` (`id`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8");


        Assert.assertFalse(existTable(mycatConnection, "db1","travelreord"));


        ////////////////////////////////////////////end/////////////////////////////////////////

        execute(mycatConnection,
                AddDatasourceHint
                        .create("dw0",
                                "jdbc:mysql://127.0.0.1:3306"));

        execute(mycatConnection,
                AddDatasourceHint
                        .create("dr0",
                                "jdbc:mysql://127.0.0.1:3306"));

        execute(mycatConnection,
                AddDatasourceHint
                        .create("dw1",
                                "jdbc:mysql://127.0.0.1:3307"));

        execute(mycatConnection,
                AddDatasourceHint
                        .create("dr1",
                                "jdbc:mysql://127.0.0.1:3307"));

        execute(mycatConnection,
                AddClusterHint
                        .create("c0",
                                Arrays.asList("dw0"), Arrays.asList("dr0")));

        execute(mycatConnection,
                AddClusterHint
                        .create("c1",
                                Arrays.asList("dw1"), Arrays.asList("dr1")));


        execute(mycatConnection, "drop table db1.travelrecord");
        execute(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `user_id` varchar(100) DEFAULT NULL,\n" +
                "  `traveldate` date DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int DEFAULT NULL,\n" +
                "  `blob` longblob,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `id` (`id`)\n" +
                ") ENGINE=InnoDB  DEFAULT CHARSET=utf8" + " BROADCAST;");

        execute(mycatConnection,"delete from db1.travelrecord");
        execute(mycatConnection,
                "insert  into db1.`travelrecord`(`id`,`user_id`,`traveldate`,`fee`,`days`,`blob`) values (12,'999',NULL,NULL,NULL,NULL);"
        );

        {
            String sql = "select * from db1.travelrecord";
            String res = executeQuery(mycatConnection, sql).toString();
            Assert.assertEquals(res, executeQuery(mysql3306, sql).toString());
            Assert.assertEquals(res, executeQuery(mysql3307, sql).toString());


        }
        execute(mycatConnection, "drop table db1.travelrecord");

        Assert.assertFalse(existTable(mycatConnection, "db1","travelreord"));


        execute(mycatConnection,"CREATE TABLE db1.`travelrecord` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `user_id` varchar(100) DEFAULT NULL,\n" +
                "  `traveldate` date DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int DEFAULT NULL,\n" +
                "  `blob` longblob,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `id` (`id`)\n" +
                ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                +" dbpartition by hash(id) tbpartition by hash(user_id) tbpartitions 2 dbpartitions 2;");

        Assert.assertTrue(existTable(mycatConnection, "db1","travelrecord"));

        execute(mycatConnection, "drop table db1.travelrecord");
        Assert.assertFalse(existTable(mycatConnection, "db1","travelrecord"));
        System.out.println();
    }

    private boolean existTable(Connection connection,String db, String table) throws SQLException {
        return !executeQuery(connection, String.format("SHOW TABLES from %s LIKE '%s';",db, table)).isEmpty();

    }

    enum Cmd {
        showSchemas("showSchemas"),
        showTables("showTables"),
        showClusters("showClusters"),
        showDatasources("showDatasources"),
        showHeartbeats("showHeartbeats"),
        showHeartbeatStatus("showHeartbeatStatus"),
        showReactors("showReactors"),
        showThreadPools("showThreadPools"),
        showNativeBackends("showNativeBackends"),
        showConnections("showConnections"),
        showSchedules("showSchedules"),
        ;
        private final String text;

        Cmd(String text) {
            this.text = text;
        }

        public String getText() {
            return text;
        }
    }

    public static abstract class HintBuilder {
        final Map<String, Object> map = new HashMap<>();

        public String build() {
            return MessageFormat.format("/* ! mycat:{0}{1} */;",
                    getCmd(),
                    JsonUtil.toJson(map));
        }

        public abstract String getCmd();
    }

    public static class AddDatasourceHint extends HintBuilder {
        private DatasourceConfig config;

        public void setDatasourceConfig(DatasourceConfig config) {
            this.config = config;
        }

        @Override
        public String getCmd() {
            return "addDatasource";
        }

        @Override
        public String build() {
            return MessageFormat.format("/*! mycat:{0}{1} */;",
                    getCmd(),
                    JsonUtil.toJson(config));
        }

        public static String create(DatasourceConfig config) {
            AddDatasourceHint addDatasourceHint = new AddDatasourceHint();
            addDatasourceHint.setDatasourceConfig(config);
            return addDatasourceHint.build();
        }

        public static String create(
                String name,
                String url
        ) {
            return create(name, "root", "123456", url);
        }

        public static String create(
                String name,
                String user,
                String password,
                String url
        ) {
            DatasourceConfig datasourceConfig = new DatasourceConfig();
            datasourceConfig.setName(name);
            datasourceConfig.setUrl(url);
            datasourceConfig.setPassword(password);
            datasourceConfig.setUser(user);
            datasourceConfig.setPassword(password);
            return create(datasourceConfig);
        }
    }

    public static class AddClusterHint extends HintBuilder {
        private ClusterConfig config;

        public static String create(String name, List<String> dsNames, List<String> ss) {
            ClusterConfig clusterConfig = new ClusterConfig();
            clusterConfig.setName(name);
            clusterConfig.setMasters(dsNames);
            clusterConfig.setReplicas(ss);

            AddClusterHint addClusterHint = new AddClusterHint();
            addClusterHint.setConfig(clusterConfig);

            return addClusterHint.build();
        }


        public void setConfig(ClusterConfig config) {
            this.config = config;
        }

        @Override
        public String getCmd() {
            return "addCluster";
        }

        @Override
        public String build() {
            return MessageFormat.format("/*! mycat:{0}{1} */;",
                    getCmd(),
                    JsonUtil.toJson(config));
        }

        public static AddClusterHint create(ClusterConfig clusterConfig) {
            AddClusterHint addClusterHint = new AddClusterHint();
            addClusterHint.setConfig(clusterConfig);
            return addClusterHint;
        }
    }

    public static class ShowSchemasHint extends HintBuilder {
        public void setSchemaName(String name) {
            map.put("schemaName", name);
        }

        @Override
        public String getCmd() {
            return "showSchemas";
        }
    }

    public static class ShowTablesHint extends HintBuilder {
        public void setGlobalType() {
            setType("global");
        }

        public void setShardingType() {
            setType("sharding");
        }

        public void setNormalType() {
            setType("normal");
        }

        public void setCustomType() {
            setType("custom");
        }

        public void setType(String name) {
            map.put("type", name);
        }

        public void setSchemaName(String name) {
            map.put("schemaName", name);
        }

        @Override
        public String getCmd() {
            return "showTables";
        }
    }

    public static class ShowClustersHint extends HintBuilder {

        public void setName(String name) {
            map.put("name", name);
        }

        @Override
        public String getCmd() {
            return "showClusters";
        }
    }

    public static class showDatasourcesHint extends HintBuilder {

        public void setName(String name) {
            map.put("name", name);
        }

        @Override
        public String getCmd() {
            return "showDatasources";
        }
    }

    public static class ShowHeartbeatsHint extends HintBuilder {
        @Override
        public String getCmd() {
            return "showHeartbeats";
        }
    }

    public static class showHeartbeatStatusHint extends HintBuilder {
        @Override
        public String getCmd() {
            return "showHeartbeatStatus";
        }
    }

    public static class ShowInstanceHint extends HintBuilder {
        @Override
        public String getCmd() {
            return "showInstances";
        }
    }

    public static class ShowReactorsHint extends HintBuilder {
        @Override
        public String getCmd() {
            return "showReactors";
        }
    }

    public static class ShowThreadPoolHint extends HintBuilder {
        @Override
        public String getCmd() {
            return "showThreadPools";
        }
    }

    public static class ShowNativeBackendHint extends HintBuilder {
        @Override
        public String getCmd() {
            return "showNativeBackends";
        }
    }

    public static class ShowConnectionsHint extends HintBuilder {
        @Override
        public String getCmd() {
            return "showConnections";
        }
    }

    public static class ShowSchedulesHint extends HintBuilder {
        @Override
        public String getCmd() {
            return "showSchedules";
        }
    }

    private void execute(Connection mySQLConnection, String sql) throws SQLException {
        JdbcUtils.execute(mySQLConnection, sql);
    }

    public static List<Map<String, Object>> executeQuery(Connection mySQLConnection, String sql) throws SQLException {
        return JdbcUtils.executeQuery(mySQLConnection, sql, Collections.emptyList());
    }

}
