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
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection db1 = getMySQLConnection(DB1);) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "create database " + " db1");
            execute(mycatConnection, "create table db1.normal_table(id int)");
            execute(mycatConnection, "create view db1.testView as select id from db1.normal_table;");
            deleteData(mycatConnection, "db1", "normal_table");
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "select * from db1.testView ");
            Assert.assertTrue(maps.isEmpty());
            deleteData(mycatConnection, "db1", "normal_table");
            execute(mycatConnection, "insert db1.normal_table (id) VALUES (1)");
            maps = executeQuery(mycatConnection, "select * from db1.testView ");
            Assert.assertTrue(!maps.isEmpty());
            Assert.assertTrue(executeQuery(db1, "show tables from db1").toString()
                    .toLowerCase().contains("testview"));
            Assert.assertTrue(executeQuery(mycatConnection, "show tables from db1").toString()
                    .toLowerCase().contains("testview"));
            execute(mycatConnection,"drop view db1.testview");
            Assert.assertTrue(!executeQuery(mycatConnection, "show tables from db1").toString()
                    .toLowerCase().contains("testview"));
            System.out.println();
        }
    }

    @Test
    public void testCreatePDistView() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection db1 = getMySQLConnection(DB1);) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "create database " + " db1");
            execute(mycatConnection, "create view db1.testView as select * from `information_schema`.`COLUMNS`;");
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "select * from db1.testView ");
            Assert.assertTrue(!maps.isEmpty());
            Assert.assertTrue(executeQuery(mycatConnection, "show tables from db1").toString()
                    .toLowerCase().contains("testview"));
            execute(mycatConnection,"drop view db1.testview");
            Assert.assertTrue(!executeQuery(mycatConnection, "show tables from db1").toString()
                    .toLowerCase().contains("testview"));
            System.out.println();
        }
    }
}
