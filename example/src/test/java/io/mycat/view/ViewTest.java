package io.mycat.view;

import io.mycat.assemble.MycatTest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class ViewTest implements MycatTest {

    @Test
    public void testCreatePhyView() throws Exception {
        try (Connection mySQLConnection = getMySQLConnection(DB_MYCAT);
             Connection db1 = getMySQLConnection(DB1);) {
            execute(mySQLConnection, RESET_CONFIG);
            execute(mySQLConnection, "create database " + " db1");
            execute(mySQLConnection, "create table db1.normal_table(id int)");
            execute(mySQLConnection, "create view db1.testView as select id from db1.normal_table;");
            deleteData(mySQLConnection, "db1", "normal_table");
            List<Map<String, Object>> maps = executeQuery(mySQLConnection, "select * from db1.testView ");
            Assert.assertTrue(maps.isEmpty());
            deleteData(mySQLConnection, "db1", "normal_table");
            execute(mySQLConnection, "insert db1.normal_table (id) VALUES (1)");
            maps = executeQuery(mySQLConnection, "select * from db1.testView ");
            Assert.assertTrue(!maps.isEmpty());
            Assert.assertTrue(executeQuery(db1, "show tables from db1").toString()
                    .toLowerCase().contains("testview"));
            Assert.assertTrue(executeQuery(mySQLConnection, "show tables from db1").toString()
                    .toLowerCase().contains("testview"));
            System.out.println();
        }
    }

    @Test
    public void testCreatePDistView() throws Exception {
        try (Connection mySQLConnection = getMySQLConnection(DB_MYCAT);
             Connection db1 = getMySQLConnection(DB1);) {
            execute(mySQLConnection, RESET_CONFIG);
            execute(mySQLConnection, "create database " + " db1");
            execute(mySQLConnection, "create view db1.testView as select * from `information_schema`.`COLUMNS`;");
            List<Map<String, Object>> maps = executeQuery(mySQLConnection, "select * from db1.testView ");
            Assert.assertTrue(!maps.isEmpty());
            Assert.assertTrue(executeQuery(mySQLConnection, "show tables from db1").toString()
                    .toLowerCase().contains("testview"));
            System.out.println();
        }
    }
}
