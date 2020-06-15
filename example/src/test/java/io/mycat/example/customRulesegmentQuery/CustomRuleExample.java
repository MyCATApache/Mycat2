package io.mycat.example.customRulesegmentQuery;

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

public class CustomRuleExample {
    @SneakyThrows
    public static void main(String[] args) {
        String resource = Paths.get(CustomRuleExample.class.getResource("").toURI()).toAbsolutePath().toString();
        System.out.println(resource);
        System.setProperty("MYCAT_HOME", resource);
        ConfigProvider bootConfig = RootHelper.INSTANCE.bootConfig(CustomRuleExample.class);
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
            statement.execute("set xa  = off");
            TestUtil.getString(statement.executeQuery("select 1"));
            statement.execute(
                    "use db1"
            );
            statement.execute(
                    "delete from travelrecord"
            );
            String string = TestUtil.getString(statement.executeQuery("explain select * from travelrecord where id = 2001"));
            Assert.assertTrue(string.contains("`travelrecord2`") || string.contains("travelrecord2 "));

            string = TestUtil.getString(statement.executeQuery("explain select * from travelrecord"));
            Assert.assertTrue(string.contains("`travelrecord`") || string.contains("travelrecord "));

            string = TestUtil.getString(statement.executeQuery("explain insert travelrecord (id) values(3)"));
            Assert.assertTrue(string.contains("travelrecord3"));

            string = TestUtil.getString(statement.executeQuery("explain select * from travelrecord where id  between 1 and 3"));
            Assert.assertTrue(string.contains("travelrecord1"));
            Assert.assertTrue(string.contains("travelrecord2"));
            Assert.assertTrue(string.contains("travelrecord3"));
        }
        if (thread != null) {
            thread.interrupt();
        }
    }
}