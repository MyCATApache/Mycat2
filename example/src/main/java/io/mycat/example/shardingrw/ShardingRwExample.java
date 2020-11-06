package io.mycat.example.shardingrw;

import io.mycat.example.ExampleObject;
import io.mycat.example.TestUtil;
import io.mycat.example.shardingxafail.ShardingXAFailExample;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public class ShardingRwExample  extends ExampleObject {
    @SneakyThrows
    public static void main(String[] args) throws Exception {
        main(args, ShardingRwExample .class);
    }

    @Test
    public void testWrapper() throws Exception {
        main(new String[]{null,null,"test","server"});
    }


    public static void test() throws Exception {
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

            statement.execute("insert db1.travelrecordWrite (`id`,`user_id`) values(1,1)");
            {

                Set<String> set = new HashSet<>();
                for (int i = 0; i < 100; i++) {
                    set.add(TestUtil.getString(statement.executeQuery(
                            "explain select * from travelrecord"
                    )));
                }
                Assert.assertTrue(set.size() > 1);//验证没有事务的情况下,可以读写分离
            }
            statement.executeUpdate("delete from db1.travelrecordWrite");
            statement.executeUpdate("delete from db1.travelrecordRead");

            Set<String> set2 = new HashSet<>();
            for (int i = 0; i < 10; i++) {
                statement.execute("INSERT INTO `db1`.`travelrecord` (`id`, `user_id`) VALUES ('1', '1'); ");
            }
          Assert.assertNotEquals("",TestUtil.getString(statement.executeQuery(
                    " select * from db1.travelrecordWrite"
            )));
            Assert.assertEquals("",TestUtil.getString(statement.executeQuery(
                    " select * from db1.travelrecordRead"
            )));

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
                        )).replaceAll("id=\\[\\d+\\]",""));
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
                            )).replaceAll("id=\\[\\d+\\]",""));
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
                    statement.execute("delete from db1.travelrecordWrite");
                    statement.execute("delete from db1.travelrecordRead");
                    statement.execute("insert db1.travelrecordWrite (`id`,`user_id`) values(1,1)");
                    for (int i = 0; i < 10; i++) {
                        set.add(TestUtil.getString(statement.executeQuery(
                                " select * from travelrecord"
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
    }
}