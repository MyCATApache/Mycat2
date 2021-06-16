package io.mycat.assemble;

import com.alibaba.druid.util.JdbcUtils;
import com.mysql.cj.jdbc.MysqlDataSource;
import io.mycat.util.NameMap;
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
}
