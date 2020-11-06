package io.mycat.example.booster;

import io.mycat.example.ExampleObject;
import io.mycat.example.TestUtil;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class BoosterExample extends ExampleObject {
    @SneakyThrows
    public static void main(String[] args) throws Exception {
        main(args, BoosterExample.class);
    }
    @Test
    public  void testWrapper() throws Exception {
        main(new String[]{null,null,"test","server"});
    }
    public static void test() throws SQLException {
        try (Connection mySQLConnection = TestUtil.getMySQLConnection()) {
            Statement statement = mySQLConnection.createStatement();
            Assert.assertTrue(TestUtil.getString(statement.executeQuery("explain select * from travelrecord"))
                    .contains("defaultDs2"));
            String string = TestUtil.getString(statement.executeQuery("SELECT COUNT(1) FROM db1.travelrecord"));
        }
    }
}