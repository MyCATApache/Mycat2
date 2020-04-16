package io.mycat.example.readWriteSeparation;

import io.mycat.ConfigProvider;
import io.mycat.MycatCore;
import io.mycat.RootHelper;
import io.mycat.example.TestUtil;
import lombok.SneakyThrows;
import org.junit.Test;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;


public class ReadWriteSeparationExample {
    @SneakyThrows
    public static void main(String[] args) throws Exception {
        String resource = Paths.get( ReadWriteSeparationExample.class.getResource("").toURI()).toAbsolutePath().toString();
        System.out.println(resource);
        System.setProperty("MYCAT_HOME", resource);
        ConfigProvider bootConfig = RootHelper.INSTANCE.bootConfig(ReadWriteSeparationExample.class);
        MycatCore.INSTANCE.init(bootConfig);
    }

    @Test
    public  void test() throws Exception {
        Thread thread = new Thread(() -> {
            try {
                main(null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread.start();
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        try (Connection mySQLConnection = TestUtil.getMySQLConnection()) {
            Statement statement = mySQLConnection.createStatement();
            statement.execute("set xa  = off");
            String string = TestUtil.getString(statement.executeQuery("select 1"));
        }
        thread.interrupt();
    }
}