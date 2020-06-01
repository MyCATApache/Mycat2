package io.mycat.example.booster;

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

public class BoosterExample {
    @SneakyThrows
    public static void main(String[] args) throws Exception {
        String resource = Paths.get(BoosterExample.class.getResource("").toURI()).toAbsolutePath().toString();
        System.out.println(resource);
        System.setProperty("MYCAT_HOME", resource);
        ConfigProvider bootConfig = RootHelper.INSTANCE.bootConfig(BoosterExample.class);
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
           Assert.assertTrue( TestUtil.getString(statement.executeQuery("explain select * from travelrecord"))
                   .contains("defaultDs2"));
            String string = TestUtil.getString(statement.executeQuery("SELECT COUNT(1) FROM db1.travelrecord"));
        }
        if (thread != null) {
            thread.interrupt();
        }
    }
}