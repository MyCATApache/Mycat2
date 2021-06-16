package io.mycat.hint;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.boot.autoconfigure.quartz.QuartzProperties;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class HintTest implements MycatTest {

    @Test(expected = java.sql.SQLException.class)
    public void testTimeout() throws Exception {
        try (Connection mySQLConnection = getMySQLConnection(DB_MYCAT);) {
            JdbcUtils.executeQuery(mySQLConnection, "/*+MYCAT:EXECUTE_TIMEOUT(1)*/ select sleep(100)", Collections.emptyList());
        }
    }

    @Test()
    public void testTimeout2() throws Exception {
        try (Connection mySQLConnection = getMySQLConnection(DB_MYCAT);) {
            JdbcUtils.executeQuery(mySQLConnection, "/*+MYCAT:EXECUTE_TIMEOUT(2000)*/ select sleep(1)", Collections.emptyList());
        }
    }

    @Test()
    public void testMasterSlave() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection master = getMySQLConnection(DB1);
             Connection slave = getMySQLConnection(DB2);) {
            JdbcUtils.execute(mycatConnection, "CREATE DATABASE if not exists db1");

            execute(mycatConnection,
                    CreateDataSourceHint
                            .create("dw1", DB1));

            execute(mycatConnection,
                    CreateDataSourceHint
                            .create("dr1", DB2));

            execute(mycatConnection,
                    CreateClusterHint
                            .create("prototype",
                                    Arrays.asList("dw1"), Arrays.asList("dr1")));


            execute(mycatConnection,
                    CreateClusterHint
                            .create("prototypeRead",
                                    Arrays.asList("dr1"), Arrays.asList("dr1")));

            JdbcUtils.execute(mycatConnection, "CREATE table if not exists db1.m (id int)");

            deleteData(master, "db1", "m");
            deleteData(slave, "db1", "m");

            JdbcUtils.executeUpdate(mycatConnection, "insert db1.m (id) values(1)", Collections.emptyList());

            for (int i = 0; i < 3; i++) {
                boolean hasData = !JdbcUtils.executeQuery(mycatConnection, "/*+MYCAT:master()*/ select 1 from db1.m limit 1", Collections.emptyList()).isEmpty();
                Assert.assertTrue(hasData);
            }
            for (int i = 0; i < 3; i++) {
                boolean hasData = !JdbcUtils.executeQuery(mycatConnection, "/*+MYCAT:slave()*/ select 1 from db1.m limit 1", Collections.emptyList()).isEmpty();
                Assert.assertFalse(hasData);
            }

            boolean hasData = !JdbcUtils.executeQuery(mycatConnection, "/*+MYCAT:node(prototypeRead)*/ select 1 from db1.m limit 1", Collections.emptyList()).isEmpty();
            Assert.assertFalse(hasData);

            hasData = !JdbcUtils.executeQuery(mycatConnection, "/*+MYCAT:TARGET(dw1)*/ select 1 from db1.m limit 1", Collections.emptyList()).isEmpty();
            Assert.assertTrue(hasData);


            hasData = !JdbcUtils.executeQuery(mycatConnection, "/*+MYCAT:TARGET(dw1,dr1)*/ select 1 from db1.m limit 1", Collections.emptyList()).isEmpty();
            Assert.assertTrue(hasData);


            System.out.println();
        }
    }

    @Test()
    public void testSharding() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection,
                    CreateDataSourceHint
                            .create("dw0", DB1));

            execute(mycatConnection,
                    CreateDataSourceHint
                            .create("dr0", DB1));

            execute(mycatConnection,
                    CreateDataSourceHint
                            .create("dw1", DB2));

            execute(mycatConnection,
                    CreateDataSourceHint
                            .create("dr1", DB2));

            execute(mycatConnection,
                    CreateClusterHint
                            .create("c0",
                                    Arrays.asList("dw0"), Arrays.asList("dr0")));

            execute(mycatConnection,
                    CreateClusterHint
                            .create("c1",
                                    Arrays.asList("dw1"), Arrays.asList("dr1")));

            execute(mycatConnection, "CREATE DATABASE db1");
            execute(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                    + " dbpartition by mod_hash(id) tbpartition by mod_hash(id) tbpartitions 2 dbpartitions 2;");
            execute(mycatConnection, "CREATE TABLE db1.`travelrecord2` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                    + " dbpartition by mod_hash(id) tbpartition by mod_hash(id) tbpartitions 2 dbpartitions 2;");
            execute(mycatConnection, "use db1");
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "explain select * from db1.travelrecord  FORCE INDEX(haha)");
            Assert.assertTrue(maps.toString().contains("haha"));
            String explain = executeQuery(mycatConnection, "explain select /*+MYCAT:scan()*/  * from db1.travelrecord").toString();
            Assert.assertTrue(explain.contains("travelrecord_0"));
            Assert.assertTrue(explain.contains("travelrecord_1"));
            Assert.assertTrue(explain.contains("travelrecord_2"));
            Assert.assertTrue(explain.contains("travelrecord_3"));

             explain = executeQuery(mycatConnection, "explain select /*+MYCAT:scan(TARGET='c0')*/  * from db1.travelrecord").toString();
            Assert.assertTrue(explain.contains("travelrecord_0"));
            Assert.assertTrue(explain.contains("travelrecord_1"));
            Assert.assertTrue(!explain.contains("travelrecord_2"));
            Assert.assertTrue(!explain.contains("travelrecord_3"));

            explain = executeQuery(mycatConnection, "explain select /*+MYCAT:scan(TARGET='c0,c1')*/  * from db1.travelrecord").toString();
            Assert.assertTrue(explain.contains("travelrecord_0"));
            Assert.assertTrue(explain.contains("travelrecord_1"));
            Assert.assertTrue(explain.contains("travelrecord_2"));
            Assert.assertTrue(explain.contains("travelrecord_3"));

            explain = executeQuery(mycatConnection, "explain select /*+MYCAT:scan(TABLE='t1', condition='t1.id = 2')*/  * from db1.travelrecord t1").toString();
            Assert.assertTrue(!explain.contains("travelrecord_0"));
            Assert.assertTrue(!explain.contains("travelrecord_1"));
            Assert.assertTrue(explain.contains("travelrecord_2"));
            Assert.assertTrue(!explain.contains("travelrecord_3"));

            explain = executeQuery(mycatConnection, "explain select /*+MYCAT:scan(TABLE='t1', PARTITION=('c0_db1_travelrecord'))*/  * from db1.travelrecord t1").toString();
            Assert.assertTrue(explain.contains("travelrecord"));
            Assert.assertTrue(!explain.contains("travelrecord_0"));
            Assert.assertTrue(!explain.contains("travelrecord_2"));
            Assert.assertTrue(!explain.contains("travelrecord_3"));

            explain = executeQuery(mycatConnection, "explain select /*+MYCAT:scan(TABLE='t1,t2', condition='t1.id = 2 and t2.id = 2')*/  * from db1.travelrecord t1 join db1.travelrecord2 t2 on t1.id = t2.id").toString();
            Assert.assertTrue(!explain.contains("travelrecord_0"));
            Assert.assertTrue(!explain.contains("travelrecord_1"));
            Assert.assertTrue(explain.contains("travelrecord_2"));
            Assert.assertTrue(!explain.contains("travelrecord_3"));

            explain = executeQuery(mycatConnection, "explain select /*+MYCAT:scan(TABLE='t1,t2', PARTITION=('c0_db1_travelrecord,c0_db1_travelrecord_5','c0_db1_travelrecord2,c0_db1_travelrecord2_0')) */  * from db1.travelrecord t1 join db1.travelrecord2 t2 on t1.id = t2.id").toString();
            Assert.assertTrue(explain.contains("db1.travelrecord_5"));
            Assert.assertTrue(explain.contains("db1.travelrecord2"));

        }
    }
}