package io.mycat.assemble;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class UserTest implements MycatTest{


    @Test
    public void testCreateUser() throws Exception {
        try (Connection connection = getMySQLConnection(8066)) {
         execute(connection, "/*+ mycat:createUser{\n" +
                    "\t\"username\":\"user\",\n" +
                    "\t\"password\":\"\",\n" +
                    "\t\"ip\":\"127.0.0.1\",\n" +
                    "\t\"transactionType\":\"xa\"\n" +
                    "} */");
            List<Map<String, Object>> maps = executeQuery(connection, "/*+ mycat:showUsers */");
            Assert.assertTrue(maps.stream().map(i -> i.get("username")).anyMatch(i -> i.equals("user")));
            execute(connection, "/*+ mycat:dropUser{\n" +
                    "\t\"username\":\"user\"" +
                    "} */");
            List<Map<String, Object>> maps2 = executeQuery(connection, "/*+ mycat:showUsers */");
            Assert.assertFalse(maps2.stream().map(i -> i.get("username")).anyMatch(i -> i.equals("user")));
            System.out.println();
        }
    }
}
