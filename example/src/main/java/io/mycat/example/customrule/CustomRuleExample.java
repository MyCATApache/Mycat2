package io.mycat.example.customrule;

import io.mycat.example.ExampleObject;
import io.mycat.example.TestUtil;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;

public class CustomRuleExample  extends ExampleObject {
    @SneakyThrows
    public static void main(String[] args) throws Exception {
        main(args, CustomRuleExample.class);
    }
    @Test
    public  void testWrapper() throws Exception {
        main(new String[]{null,null,"test","server"});
    }

    public static void test() throws Exception {
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

            Assert.assertTrue(string.contains("travelrecord"));
            Assert.assertFalse(string.contains("travelrecord1"));
            Assert.assertFalse(string.contains("travelrecord2"));
            Assert.assertFalse(string.contains("travelrecord3"));
        }
    }
}