package io.mycat.alter;

import io.mycat.assemble.MycatTest;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class DropTableTest implements MycatTest {
    @Test
    public void testNormal() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "CREATE DATABASE db1");
            execute(mycatConnection, "CREATE TABLE db1.`travelrecord2` (\n" +
                    "  `id` bigint(20) NOT NULL KEY " +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n");
            execute(mycatConnection, "drop  TABLE db1.`travelrecord2`");
            Assert.assertFalse(existTable(mycatConnection, "db1", "travelrecord2"));
        }
    }
    @Test
    public void testGlobal() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));
            execute(mycatConnection, CreateDataSourceHint
                    .create("ds1",
                            DB2));
            execute(mycatConnection, CreateClusterHint.create("c0",
                    Arrays.asList("ds1"), Collections.emptyList()));

            execute(mycatConnection, "CREATE DATABASE db1");
            execute(mycatConnection, "CREATE TABLE db1.`travelrecord2` (\n" +
                    "  `id` bigint(20) NOT NULL KEY " +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4  broadcast");
            execute(mycatConnection, "drop  TABLE db1.`travelrecord2`");
            Assert.assertFalse(existTable(mycatConnection, "db1", "travelrecord2"));
        }
    }
    @Test
    public void testSharding() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));
            execute(mycatConnection, CreateDataSourceHint
                    .create("ds1",
                            DB2));
            execute(mycatConnection, CreateClusterHint.create("c0",
                    Arrays.asList("ds1"), Collections.emptyList()));

            execute(mycatConnection, "CREATE DATABASE db1");
            execute(mycatConnection, "CREATE TABLE db1.`travelrecord2` (\n" +
                    "  `id` bigint(20) NOT NULL KEY " +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2 dbpartitions 2;\n");
            execute(mycatConnection, "drop  TABLE db1.`travelrecord2`");
            Assert.assertFalse(existTable(mycatConnection, "db1", "travelrecord2"));
        }
    }
}
