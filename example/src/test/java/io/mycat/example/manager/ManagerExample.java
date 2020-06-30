package io.mycat.example.manager;

import com.rits.cloning.Cloner;
import io.mycat.*;
import io.mycat.config.ShardingQueryRootConfig;
import io.mycat.example.TestUtil;
import io.mycat.hbt.TextConvertor;
import io.mycat.util.NetUtil;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 读写分离例子不能使用全局序列号,jdbc系统,读写分离也没有预处理的支持
 */
public class ManagerExample {
    @SneakyThrows
    public static void main(String[] args) throws Exception {
        String resource = Paths.get(ManagerExample.class.getResource("").toURI()).toAbsolutePath().toString();
        System.out.println(resource);
        System.setProperty("MYCAT_HOME", resource);
        ConfigProvider bootConfig = RootHelper.INSTANCE.bootConfig(ManagerExample.class);
        MycatCore.INSTANCE.init(bootConfig);
    }

    @Test
    public void test() throws Exception {
        String resource = Paths.get(ManagerExample.class.getResource("").toURI()).toAbsolutePath().toString();
        System.out.println(resource);
        System.setProperty("MYCAT_HOME", resource);

        FileConfigProvider fileConfigProvider = (FileConfigProvider) RootHelper.INSTANCE.bootConfig(ManagerExample.class);
        MycatConfig oldConfig = fileConfigProvider.currentConfig();

        MycatHttpConfigServer mycatHttpConfigServer = MycatHttpConfigServer.INSTANCE;
        MycatConfig backup = Cloner.standard().deepClone(fileConfigProvider.currentConfig());
        mycatHttpConfigServer.setConfig(backup);
        mycatHttpConfigServer.setGlobalVariables(fileConfigProvider.globalVariables());
        mycatHttpConfigServer.start();
        System.setProperty("MYCAT_CONFIG_PROVIER", HttpConfigProvider.class.getName());
        RootHelper.INSTANCE.bootConfig(ManagerExample.class);

        String defaultPath = fileConfigProvider.getDefaultPath();

        ShardingQueryRootConfig.LogicSchemaConfig logicSchemaConfig = new ShardingQueryRootConfig.LogicSchemaConfig();
        logicSchemaConfig.setSchemaName("testdb");
        backup.getMetadata().getSchemas().add(logicSchemaConfig);

        Thread thread = null;
        if (!NetUtil.isHostConnectable("0.0.0.0", 9066)) {
            thread = new Thread(() -> {
                try {
                    main(null);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        if (thread != null) {
            thread.start();
            Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        }
        String sql;
        String expected;
        try (Connection mySQLConnection = TestUtil.getMySQLConnection(9066)) {
            try (Statement statement = mySQLConnection.createStatement()) {

                //基础监控命令仅仅测试列名有没有变动
                sql = "show @@connection";
                expected = "ID,USER_NAME,HOST,SCHEMA,AFFECTED_ROWS,AUTOCOMMIT,IN_TRANSACTION,CHARSET,CHARSET_INDEX,OPEN,SERVER_CAPABILITIES,ISOLATION,LAST_ERROR_CODE,LAST_INSERT_ID,LAST_MESSAGE,PROCESS_STATE,WARNING_COUNT,MYSQL_SESSION_ID,TRANSACTION_TYPE,TRANSCATION_SNAPSHOT,CANCEL_FLAG";
                matchMetadata(statement, sql, expected);

                sql = "show @@backend.native";
                expected = "SESSION_ID,THREAD_NAME,THREAD_ID,DS_NAME,LAST_MESSAGE,MYCAT_SESSION_ID,IS_IDLE,SELECT_LIMIT,IS_AUTOCOMMIT,IS_RESPONSE_FINISHED,RESPONSE_TYPE,IS_IN_TRANSACTION,IS_REQUEST_SUCCESS,IS_READ_ONLY";
                matchMetadata(statement, sql, expected);

                sql = "show @@backend.datasource";
                expected = "NAME,IP,PORT,USERNAME,PASSWORD,MAX_CON,MIN_CON,EXIST_CON,USE_CON,MAX_RETRY_COUNT,MAX_CONNECT_TIMEOUT,DB_TYPE,URL,WEIGHT,INIT_SQL,INIT_SQL_GET_CONNECTION,INSTANCE_TYPE,IDLE_TIMEOUT,DRIVER,TYPE,IS_MYSQL";
                matchMetadata(statement, sql, expected);

                sql = "show @@backend.heartbeat";
                expected = "NAME,TYPE,READABLE,SESSION_COUNT,WEIGHT,ALIVE,MASTER,HOST,PORT,LIMIT_SESSION_COUNT,REPLICA,SLAVE_THRESHOLD,IS_HEARTBEAT_TIMEOUT,HB_ERROR_COUNT,HB_LAST_SWITCH_TIME,HB_MAX_RETRY,IS_CHECKING,MIN_SWITCH_TIME_INTERVAL,HEARTBEAT_TIMEOUT,SYNC_DS_STATUS,HB_DS_STATUS,IS_SLAVE_BEHIND_MASTER,LAST_SEND_QUERY_TIME,LAST_RECEIVED_QUERY_TIME";
                matchMetadata(statement, sql, expected);

                sql = "show @@help";
                expected = "STATEMENT,DESCRIPTION";
                matchMetadata(statement, sql, expected);

                sql = "show @@backend.instance";
                expected = "NAME,ALIVE,READABLE,TYPE,SESSION_COUNT,WEIGHT,MASTER,HOST,PORT,LIMIT_SESSION_COUNT,REPLICA";
                matchMetadata(statement, sql, expected);

                sql = "show @@metadata.schema";
                expected = "SCHEMA_NAME,DEFAULT_TARGET_NAME,TABLE_NAMES";
                matchMetadata(statement, sql, expected);

                sql = "show @@metadata.schema.table";
                expected = "SCHEMA_NAME,TABLE_NAME,CREATE_TABLE_SQL,TYPE,COLUMNS";
                matchMetadata(statement, sql, expected);

                sql = "show @@reactor";
                expected = "THREAD_NAME,THREAD_ID,CUR_SESSION_ID,PREPARE_STOP,BUFFER_POOL_SNAPSHOT,LAST_ACTIVE_TIME";
                matchMetadata(statement, sql, expected);


                sql = "show @@backend.replica";
                expected = "NAME,SWITCH_TYPE,MAX_REQUEST_COUNT,TYPE,WRITE_DS,READ_DS,WRITE_L,READ_L,AVAILABLE";
                matchMetadata(statement, sql, expected);

//                sql = "show @@stat";
//                expected = "STATEMENT,START_TIME,END_TIME,SQL_ROWS,NET_IN_BYTES,NET_OUT_BYTES,PARSE_TIME,COMPILE_TIME,CBO_TIME,RBO_TIME,CONNECTION_POOL_TIME,CONNECTION_QUERY_TIME";
//
//                matchMetadata(statement, sql, expected);

                sql = "show @@threadPool";
                expected = "NAME,POOL_SIZE,ACTIVE_COUNT,TASK_QUEUE_SIZE,COMPLETED_TASK,TOTAL_TASK";
                matchMetadata(statement, sql, expected);


                //因为配置是关闭心跳的,所以返回值是0(false)
                Assert.assertEquals("(0)", TextConvertor.dumpResultSet(statement.executeQuery("show @@backend.heartbeat.running")));
                //开启心跳设置,当配置是false的时候,也会自动设置为true
                statement.execute("switch @@backend.heartbeat = true");
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
                //观察到心跳日志
                Assert.assertEquals("(1)", TextConvertor.dumpResultSet(statement.executeQuery("show @@backend.heartbeat.running")));
                //关闭心跳,但是不会把配置中的心跳配置设置成false
                statement.execute("switch @@backend.heartbeat = false");

                //确认已经关闭心跳
                Assert.assertEquals("(0)", TextConvertor.dumpResultSet(statement.executeQuery("show @@backend.heartbeat.running")));

                //查询实例状态
                ResultSet resultSet = statement.executeQuery("show @@backend.instance");

                ArrayList<String> rows = new ArrayList<>();
                while (resultSet.next()) {
                    Object[] objects = {
                            resultSet.getObject("NAME"), resultSet.getObject("ALIVE"), resultSet.getObject("READABLE")
                    };
                    String s = Arrays.toString(objects);
                    rows.add(s);
                }
                //所以实例都是正常的
                Assert.assertEquals("[[defaultDs2, 1, 1], [defaultDs, 1, 1]]", rows.toString());
                //设置两个实例不可用
                statement.execute("switch @@backend.instance = {name:'defaultDs2' ,alive:'false' ,readable:'false'}");
                statement.execute("switch @@backend.instance = {name:'defaultDs' ,alive:'false' ,readable:'false'}");
                //所以实例都是不可用的
                resultSet = statement.executeQuery("show @@backend.instance");
                rows = new ArrayList<>();
                while (resultSet.next()) {
                    Object[] objects = {
                            resultSet.getObject("NAME"), resultSet.getObject("ALIVE"), resultSet.getObject("READABLE")
                    };
                    String s = Arrays.toString(objects);
                    rows.add(s);
                }
                //设置第二个实例是可用的
                Assert.assertEquals("[[defaultDs2, 0, 0], [defaultDs, 0, 0]]", rows.toString());
                //确认设置生效
                statement.execute("switch @@backend.instance = {name:'defaultDs2' ,alive:'true' ,readable:'true'}");
                //触发集群切换
                statement.execute("switch @@backend.replica = {name:'repli'}");
                String replInfo = TextConvertor.dumpResultSet(statement.executeQuery("show @@backend.replica"));
                //确认切换完成
                Assert.assertFalse(replInfo.contains("defaultDs,"));

                System.out.println("");
                //配置更新测试

                MycatConfig newConfig = Cloner.standard().deepClone(fileConfigProvider.currentConfig());
                List<ShardingQueryRootConfig.LogicSchemaConfig> schemas = newConfig.getMetadata().getSchemas();
                ShardingQueryRootConfig.LogicSchemaConfig logicSchemaConfig1 = new ShardingQueryRootConfig.LogicSchemaConfig();
                schemas.add(logicSchemaConfig1);
                logicSchemaConfig1.setSchemaName("TESTDB");
                mycatHttpConfigServer.setConfig(newConfig);
                statement.execute("reload @@config by file");

                String show_databases = TestUtil.getString(statement.executeQuery("show databases"));
                Assert.assertTrue(show_databases.contains("TESTDB"));

                TestUtil.getString(statement.executeQuery("show @@stat"));
                statement.execute("reset @@stat");
            }

            //kill 命令测试,检查kill之后旧连接是否存在
            ArrayList<String> ids = new ArrayList<>();
            ArrayList<String> ids2 = new ArrayList<>();
            try (Connection connection = TestUtil.getMySQLConnection(9066)) {
                Connection mySQLConnection1 = TestUtil.getMySQLConnection(8066);//创建8066连接,以便后面杀死
                ResultSet resultSet1 = connection.createStatement().executeQuery("show @@connection");
                while (resultSet1.next()) {
                    ids.add(resultSet1.getString("ID"));
                }
                sql = "kill @@connection " + String.join(",", ids);
                connection.createStatement().execute(sql);
            }
            try (Connection connection = TestUtil.getMySQLConnection(9066)) {
                ResultSet resultSet2 = connection.createStatement().executeQuery("show @@connection");
                while (resultSet2.next()) {
                    ids2.add(resultSet2.getString("ID"));
                }
            }

            ids2.retainAll(ids);
            Assert.assertTrue(ids2.isEmpty());//没有交集,为空

            Object content = new URL("http://127.0.0.1:7066/metrics").openConnection().getContent();
            Assert.assertNotNull(content);
            System.out.println(content);
        }
        if (thread != null) {
            thread.interrupt();
        }
    }

    private void matchMetadata(Statement statement, String sql, String expected) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            String text = TextConvertor.dumpMetadata(resultSet.getMetaData());
            System.out.println(sql);
            System.out.println(text);
            System.out.println("expected = " + "\"" + text + "\";");
            Assert.assertEquals(expected, text);
        }
    }

}