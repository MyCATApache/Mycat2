package io.mycat.connection;

import io.mycat.assemble.MycatTest;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class BackendConnectionTest implements MycatTest {
    @Test
    public void testPrototypeNoTranscationSelect() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, RESET_CONFIG);
            repeatSql(mycatConnection,  "SELECT * FROM `mysql`.`role_edges` LIMIT 0, 1000; ",400);
            Assert.assertEquals(0,getUseCon(mycatConnection,"prototypeDs"));
        }
    }
    @Test
    public void testPrototypeTranscationSelectCommit() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, RESET_CONFIG);
            mycatConnection.setAutoCommit(false);
            repeatSql(mycatConnection,  "SELECT * FROM `mysql`.`role_edges` LIMIT 0, 1000; ",400);
            Assert.assertEquals(1,getUseCon(mycatConnection,"prototypeDs"));
            mycatConnection.commit();
            Thread.sleep(5);
            Assert.assertEquals(0,getUseCon(mycatConnection,"prototypeDs"));
        }
    }
    @Test
    public void testPrototypeTranscationSelectRollback() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, RESET_CONFIG);
            mycatConnection.setAutoCommit(false);
            repeatSql(mycatConnection,  "SELECT * FROM `mysql`.`role_edges` LIMIT 0, 1000; ",400);
            Assert.assertEquals(1,getUseCon(mycatConnection,"prototypeDs"));
            mycatConnection.rollback();
            Thread.sleep(5);
            Assert.assertEquals(0,getUseCon(mycatConnection,"prototypeDs"));
        }
    }
    @Test
    public void testPrototypeTranscationSelectSetAutocommit() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, RESET_CONFIG);
            mycatConnection.setAutoCommit(false);
            repeatSql(mycatConnection,  "SELECT * FROM `mysql`.`role_edges` LIMIT 0, 1000; ",400);
            Assert.assertEquals(1,getUseCon(mycatConnection,"prototypeDs"));
            mycatConnection.setAutoCommit(true);
            Thread.sleep(5);
            Assert.assertEquals(0,getUseCon(mycatConnection,"prototypeDs"));
        }
    }
    @Test
    public void testShardingNoTranscationSelect() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            initTestData(mycatConnection);
            repeatSql(mycatConnection,  "SELECT * FROM `db1`.`travelrecord2` LIMIT 0, 1000; ",400);
            Assert.assertEquals(0,getUseCon(mycatConnection,"prototypeDs"));
            Assert.assertEquals(0,getUseCon(mycatConnection,"ds0"));
            Assert.assertEquals(0,getUseCon(mycatConnection,"ds1"));
        }
    }
    @Test
    public void testShardingTranscationSelectCommit() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            initTestData(mycatConnection);
            mycatConnection.setAutoCommit(false);
            repeatSql(mycatConnection,  "SELECT * FROM `db1`.`travelrecord2` LIMIT 0, 1000; ",400);
            Assert.assertEquals(0,getUseCon(mycatConnection,"prototypeDs"));
            Assert.assertEquals(1,getUseCon(mycatConnection, "ds0"));
            Assert.assertEquals(1,getUseCon(mycatConnection,"ds1"));
            mycatConnection.commit();
            Thread.sleep(5);
            Assert.assertEquals(0,getUseCon(mycatConnection,"prototypeDs"));
            Assert.assertEquals(0,getUseCon(mycatConnection,"ds0"));
            Assert.assertEquals(0,getUseCon(mycatConnection,"ds1"));
        }
    }
    @Test
    public void testShardingTranscationSelectRollback() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            initTestData(mycatConnection);
            mycatConnection.setAutoCommit(false);
            repeatSql(mycatConnection,  "SELECT * FROM `db1`.`travelrecord2` LIMIT 0, 1000; ",400);
            Assert.assertEquals(0,getUseCon(mycatConnection,"prototypeDs"));
            Assert.assertEquals(1,getUseCon(mycatConnection,"ds0"));
            Assert.assertEquals(1,getUseCon(mycatConnection,"ds1"));
            mycatConnection.rollback();
            Thread.sleep(5);
            Assert.assertEquals(0,getUseCon(mycatConnection,"prototypeDs"));
            Assert.assertEquals(0,getUseCon(mycatConnection,"ds0"));
            Assert.assertEquals(0,getUseCon(mycatConnection,"ds1"));
        }
    }
    @Test
    public void testShardingTranscationSelectSetAutocommit() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            initTestData(mycatConnection);
            mycatConnection.setAutoCommit(false);
            repeatSql(mycatConnection,  "SELECT * FROM `db1`.`travelrecord2` LIMIT 0, 1000; ",400);
            Assert.assertEquals(0,getUseCon(mycatConnection,"prototypeDs"));
            Assert.assertEquals(1,getUseCon(mycatConnection,"ds0"));
            Assert.assertEquals(1,getUseCon(mycatConnection,"ds1"));
            mycatConnection.setAutoCommit(true);
            Thread.sleep(5);
            Assert.assertEquals(0,getUseCon(mycatConnection,"prototypeDs"));
            Assert.assertEquals(0,getUseCon(mycatConnection,"ds0"));
            Assert.assertEquals(0,getUseCon(mycatConnection,"ds1"));
        }
    }
    private void initTestData(Connection mycatConnection) throws Exception {
        execute(mycatConnection, RESET_CONFIG);

        execute(mycatConnection, "DROP DATABASE db1");


        execute(mycatConnection, "CREATE DATABASE db1");


        execute(mycatConnection, CreateDataSourceHint
                .create("ds0",
                        DB1));
        execute(mycatConnection, CreateDataSourceHint
                .create("ds1",
                        DB1));


        execute(mycatConnection,
                CreateClusterHint.create("c0",
                        Arrays.asList("ds0"), Collections.emptyList()));
        execute(mycatConnection,
                CreateClusterHint.create("c1",
                        Arrays.asList("ds1"), Collections.emptyList()));

        execute(mycatConnection, "USE `db1`;");

        execute(mycatConnection, "CREATE TABLE `travelrecord2` (\n" +
                "  `id` bigint(20) NOT NULL KEY,\n" +
                "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                "  `traveldate` datetime(6) DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int(11) DEFAULT NULL,\n" +
                "  `blob` longblob DEFAULT NULL\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "tbpartition by YYYYMM(traveldate) tbpartitions 12;");
    }

    @Test
    public void testShardingTranscationSelect() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            initTestData(mycatConnection);

            repeatSql(mycatConnection,  "SELECT * FROM `db1`.`travelrecord2` LIMIT 0, 1000; ",400);
            Assert.assertEquals(0,getUseCon(mycatConnection,"prototypeDs"));
            Assert.assertEquals(0,getUseCon(mycatConnection,"ds0"));
            Assert.assertEquals(0,getUseCon(mycatConnection,"ds1"));
        }
    }
    private long getUseCon(Connection connection,String dsName) throws Exception {
        List<Map<String, Object>> maps = executeQuery(connection, "/*+ mycat:showDataSources{} */");
        return maps.stream().filter(r -> dsName.equalsIgnoreCase((String) r.get("NAME"))).map(r -> (Number) r.get("USE_CON")).findFirst().get().longValue();
    }



    private void repeatSql(Connection mycatConnection, String sql, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            executeQuery(mycatConnection, sql);
        }
    }
}