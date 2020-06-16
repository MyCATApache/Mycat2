package io.mycat.example.shardingXAFail;

import io.mycat.ConfigProvider;
import io.mycat.MycatCore;
import io.mycat.RootHelper;
import io.mycat.example.TestUtil;
import io.mycat.util.NetUtil;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

public class ShardingXAFailExample {
    @SneakyThrows
    public static void main(String[] args) {
        String resource = Paths.get(ShardingXAFailExample.class.getResource("").toURI()).toAbsolutePath().toString();
        System.out.println(resource);
        System.setProperty("MYCAT_HOME", resource);
        ConfigProvider bootConfig = RootHelper.INSTANCE.bootConfig(ShardingXAFailExample.class);
        MycatCore.INSTANCE.init(bootConfig);
    }

    @Test
    public void test() throws Exception {
        Thread thread = null;
        if (!NetUtil.isHostConnectable("0.0.0.0", 8066)) {
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
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));
        }

        try (Connection mySQLConnection = TestUtil.getMySQLConnection()) {
            Statement statement = mySQLConnection.createStatement();
            statement.execute("set xa  = on");
            statement.execute(
                    "use db1"
            );
            statement.execute(
                    "delete from travelrecord"
            );
            mySQLConnection.setAutoCommit(false);
            statement.execute("INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('9999999999');");
            statement.execute("INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('1');");
            statement.execute("INSERT INTO `db1`.`travelrecord` (`id`,`user_id`) VALUES ('1',999/0);");
        } catch (Throwable e) {
            e.printStackTrace();
        }
        try (Connection mySQLConnection = TestUtil.getMySQLConnection()) {
            Statement statement = mySQLConnection.createStatement();
            statement.execute("set xa  = on");

            statement.execute(
                    "use db1"
            );
            Assert.assertEquals("", TestUtil.getString(statement.executeQuery(
                    "select *  from travelrecord"
            )));
            mySQLConnection.setAutoCommit(false);
            statement.execute("INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('9999999999');");
            statement.execute("INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('1');");
            mySQLConnection.commit();
            Assert.assertNotEquals("", TestUtil.getString(statement.executeQuery(
                    "select *  from travelrecord"
            )));
        }
        if (thread != null) {
            thread.interrupt();
        }
    }
}