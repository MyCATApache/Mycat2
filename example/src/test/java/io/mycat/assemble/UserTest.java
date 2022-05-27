package io.mycat.assemble;

import com.alibaba.druid.util.JdbcUtils;
import com.mysql.cj.jdbc.MysqlDataSource;
import io.mycat.config.ShardingBackEndTableInfoConfig;
import io.mycat.config.ShardingFunction;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.hint.CreateTableHint;
import io.mycat.router.custom.HttpCustomRuleFunction;
import io.mycat.router.mycat1xfunction.PartitionByRangeMod;
import io.mycat.util.NameMap;
import org.apache.groovy.util.Maps;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class UserTest implements MycatTest {


    @Test
    public void testCreateUser() throws Exception {
        try (Connection connection = getMySQLConnection(DB_MYCAT);) {
            execute(connection, RESET_CONFIG);
            execute(connection, "/*+ mycat:createUser{\n" +
                    "\t\"username\":\"user\",\n" +
                    "\t\"password\":\"\",\n" +
                    "\t\"ip\":\"127.0.0.1\",\n" +
                    "\t\"transactionType\":\"xa\"\n" +
                    "} */");
            List<Map<String, Object>> maps = executeQuery(connection, "/*+ mycat:showUsers */");
            Assert.assertTrue(maps.stream().map(i -> i.get("username")).anyMatch(i -> i.equals("user")));
            execute(connection, "/*+ mycat:dropUser{\n" +
                    "\t\"username\":\"user\"" +
                    "} */");
            List<Map<String, Object>> maps2 = executeQuery(connection, "/*+ mycat:showUsers */");
            Assert.assertFalse(maps2.stream().map(i -> i.get("username")).anyMatch(i -> i.equals("user")));
            System.out.println();
        }
    }

    @Test
    public void testKill() throws Exception {
        try (Connection connection = getMySQLConnection(DB_MYCAT);) {
            Connection connection2 = getMySQLConnection(DB_MYCAT);
            List<Map<String, Object>> maps1 = JdbcUtils.executeQuery(connection2, "SELECT CONNECTION_ID();", Collections.emptyList());
            String id = maps1.get(0).values().iterator().next().toString();
            CountDownLatch latch = new CountDownLatch(1);
            new Thread(() -> {
                try {
                    Thread.sleep(200);
                    JdbcUtils.executeQuery(connection2, "select sleep(10000)", Collections.emptyList());
                } catch (Throwable throwable) {
                    latch.countDown();
                }
            }).start();
            Thread.sleep(1000);
            JdbcUtils.execute(connection, "kill " + id);
            latch.await(30, TimeUnit.SECONDS);
            Assert.assertTrue(latch.getCount() == 0);
        }
    }

    @Test
    public void testHttpFunction() throws Exception {
        try (Connection mycat = getMySQLConnection(DB_MYCAT);
             Connection prototypeMysql = getMySQLConnection(DB1);) {
            execute(mycat, RESET_CONFIG);
            String db = "testSchema";
            String tableName = "sharding";
            execute(mycat, "drop database if EXISTS " + db);
            execute(mycat, "create database " + db);
            execute(mycat, "use " + db);

            execute(mycat, CreateDataSourceHint
                    .create("dw0", DB1));
            execute(mycat, CreateDataSourceHint
                    .create("dw1", DB2));

            execute(prototypeMysql, "use mysql");

            String shardingConfig = CreateTableHint
                    .createSharding(db, tableName,
                            "create table " + tableName + "(\n" +
                                    "id int(11) NOT NULL AUTO_INCREMENT,\n" +
                                    "user_id int(11) ,\n" +
                                    "user_name varchar(128), \n" +
                                    "PRIMARY KEY (`id`), \n" +
                                    " GLOBAL INDEX `g_i_user_id`(`user_id`) COVERING (`user_name`) dbpartition by btree(`user_id`) \n" +
                                    ")ENGINE=InnoDB DEFAULT CHARSET=utf8 ",
                            ShardingBackEndTableInfoConfig.builder().build(),
                            ShardingFunction.builder()
                                    .clazz(HttpCustomRuleFunction.class.getCanonicalName())
                                    .properties(Maps.of(
                                            "name", "test",
                                            "shardingDbKeys", "",
                                            "shardingTableKeys", "id",
                                            "shardingTargetKeys", "",
                                            "allScanPartitionTimeout", 5,
                                            "fetchTimeout", 60000,
                                            "routerServiceAddress", "http://127.0.0.1:9066/router_service_address"))
                                    .build());
            execute(
                    mycat,shardingConfig
                   );
            hasData(mycat,db,tableName);
            System.out.println(shardingConfig);
        }
    }
}
