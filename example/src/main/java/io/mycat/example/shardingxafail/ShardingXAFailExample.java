package io.mycat.example.shardingxafail;

import io.mycat.example.ExampleObject;
import io.mycat.example.TestUtil;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;

public class ShardingXAFailExample  extends ExampleObject {
    @SneakyThrows
    public static void main(String[] args) throws Exception {
        main(args, ShardingXAFailExample .class);
    }

    @Test
    public void testWrapper() throws Exception {
        main(new String[]{null,null,"test","server"});
    }

    public static void test() throws Exception {

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
    }
}