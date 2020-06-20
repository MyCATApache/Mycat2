package io.mycat.example.manager;

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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 读写分离例子不能使用全局序列号,jdbc系统,读写分离也没有预处理的支持
 */
public class ReadWriteSeparationExample {
    @SneakyThrows
    public static void main(String[] args) throws Exception {
        String resource = Paths.get(ReadWriteSeparationExample.class.getResource("").toURI()).toAbsolutePath().toString();
        System.out.println(resource);
        System.setProperty("MYCAT_HOME", resource);
        ConfigProvider bootConfig = RootHelper.INSTANCE.bootConfig(ReadWriteSeparationExample.class);
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
            String string = TestUtil.getString(statement.executeQuery("select 1"));
            statement.execute(
                    "use db1"
            );
            statement.execute(
                    "delete from travelrecord"
            );
            {
                Set<String> set = new HashSet<>();
                for (int i = 0; i < 10; i++) {
                    set.add(TestUtil.getString(statement.executeQuery(
                            "explain select * from travelrecord"
                    )));
                }
                Assert.assertTrue(set.size() > 1);//验证没有事务的情况下,可以读写分离
            }
            Set<String> set2 = new HashSet<>();
            for (int i = 0; i < 10; i++) {
                set2.add(TestUtil.getString(statement.executeQuery(
                        "explain INSERT INTO `db1`.`travelrecord` (`id`, `user_id`) VALUES ('1', '1'); "
                )));
            }
            Assert.assertEquals(1, set2.size());//验证插入语句只到主节点


            //验证能返回自增序列
            String lastInsertId = null;
            for (int i = 0; i < 10; i++) {
                Assert.assertEquals(1, statement.executeUpdate("INSERT INTO `db1`.`travelrecord` (`id`,`user_id`) VALUES ('1','1');", Statement.RETURN_GENERATED_KEYS));
                try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                    lastInsertId = TestUtil.getString(generatedKeys);
                    Assert.assertEquals("(1)", lastInsertId);
                }
            }

            Assert.assertTrue(lastInsertId.equalsIgnoreCase(TestUtil.getString(statement.executeQuery("SELECT  LAST_INSERT_ID() "))));

            //测试autocommit测试
            {
                Assert.assertEquals("(1)", TestUtil.getString(statement.executeQuery("SELECT @@session.autocommit")));
                mySQLConnection.setAutoCommit(false);
                Assert.assertEquals("(0)", TestUtil.getString(statement.executeQuery("SELECT @@session.autocommit")));//set autocommit下一个语句进入事务状态
                {
                    Set<String> set = new HashSet<>();
                    for (int i = 0; i < 10; i++) {
                        set.add(TestUtil.getString(statement.executeQuery(
                                "explain select * from travelrecord"
                        )));
                    }
                    Assert.assertTrue(set.size() == 1);//验证有事务的情况下,不读写分离
                }
                mySQLConnection.rollback();
                {
                    Assert.assertEquals("(0)", TestUtil.getString(statement.executeQuery("SELECT @@session.autocommit")));//set autocommit这个语句进入事务状态
                    {
                        Set<String> set = new HashSet<>();
                        for (int i = 0; i < 10; i++) {
                            set.add(TestUtil.getString(statement.executeQuery(
                                    "explain select * from travelrecord"
                            )));
                        }
                        Assert.assertEquals(1, set.size());//验证无事务的情况下但是set autocommit = 0,不读写分离
                    }
                    mySQLConnection.rollback();
                }
                mySQLConnection.setAutoCommit(false);
                mySQLConnection.setAutoCommit(true);
            /*
            SET autocommit = 0;
            SET autocommit = 1;不会进入事务
             */
                {
                    Set<String> set = new HashSet<>();
                    for (int i = 0; i < 10; i++) {
                        set.add(TestUtil.getString(statement.executeQuery(
                                "explain select * from travelrecord"
                        )));
                    }
                    Assert.assertTrue(set.size() > 1);//验证无事务的情况下但是set autocommit = 1,读写分离
                }

            }

            //事务测试
            {
                statement.execute("delete from travelrecord");
                mySQLConnection.setAutoCommit(false);


                {
                    Set<String> set = new HashSet<>();
                    for (int i = 0; i < 10; i++) {
                        set.add(TestUtil.getString(statement.executeQuery(
                                "explain select * from travelrecord"
                        )));
                    }
                    Assert.assertTrue(set.size() == 1);//验证有事务的情况下,不读写分离
                }

                {
                    statement.execute("INSERT INTO `db1`.`travelrecord` (`id`,`user_id`) VALUES ('1','1');");
                    mySQLConnection.rollback();
                }

                {
                    Assert.assertEquals("", TestUtil.getString(statement.executeQuery("select * from travelrecord")));
                    statement.execute("INSERT INTO `db1`.`travelrecord` (`id`,`user_id`) VALUES ('1','1');");
                    mySQLConnection.commit();
                    Assert.assertNotEquals("()", TestUtil.getString(statement.executeQuery("select * from travelrecord")));
                }

            }
        }
        if (thread != null) {
            thread.interrupt();
        }
    }
}