package io.mycat.example.pstmt;

import io.mycat.ConfigProvider;
import io.mycat.MycatCore;
import io.mycat.RootHelper;
import io.mycat.example.TestUtil;
import io.mycat.hbt.TextConvertor;
import io.mycat.util.NetUtil;
import lombok.SneakyThrows;
import org.junit.Test;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class PstmtShardingExample {
    @SneakyThrows
    public static void main(String[] args) {
        String resource = Paths.get(PstmtShardingExample.class.getResource("").toURI()).toAbsolutePath().toString();
        System.out.println(resource);
        System.setProperty("MYCAT_HOME", resource);
        ConfigProvider bootConfig = RootHelper.INSTANCE.bootConfig(PstmtShardingExample.class);
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
        try (Connection mySQLConnection = TestUtil.getPstmtMySQLConnection()) {
            PreparedStatement preparedStatement = mySQLConnection.prepareStatement("INSERT INTO `db1`.`travelrecord` (`id`,`user_id`) VALUES (?,?);");
            ThreadLocalRandom current = ThreadLocalRandom.current();
            for (int i = 0; i < 600; i++) {
                preparedStatement.setInt(1,i);
                preparedStatement.setInt(2,current.nextInt());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        }
        try (Connection mySQLConnection = TestUtil.getPstmtMySQLConnection()) {
            PreparedStatement preparedStatement = mySQLConnection.prepareStatement("select * from db1.travelrecord where id =? and user_id = ?");
            preparedStatement.setInt(1, 1);
            preparedStatement.setInt(2, 2);
            String s = TextConvertor.dumpResultSet(preparedStatement.executeQuery());
            System.out.println(s);
        }
        try (Connection mySQLConnection = TestUtil.getPstmtMySQLConnection()) {
            PreparedStatement preparedStatement = mySQLConnection.prepareStatement("select 1");
            String s = TextConvertor.dumpResultSet(preparedStatement.executeQuery());
            System.out.println(s);
        }

        if (thread != null) {
            thread.interrupt();
        }
    }
}