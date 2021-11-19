package io.mycat.alter;

import com.alibaba.druid.util.JdbcUtils;
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
public class AlterTableTest implements MycatTest {
    @Test
    public void testNormal() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection db1 = getMySQLConnection(DB1)) {
            execute(mycatConnection, RESET_CONFIG);
            JdbcUtils.execute(db1,
                    "drop TABLE if exists db1.`travelrecord2`");

            execute(mycatConnection, "CREATE DATABASE db1");
            execute(mycatConnection, "CREATE TABLE db1.`travelrecord2` (\n" +
                    "  `id` bigint(20) NOT NULL KEY " +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n");

            Assert.assertEquals(1,
                    getColumns(mycatConnection, "db1", "travelrecord2")
                            .getColumnCount());
            Assert.assertEquals(1,
                    getColumns(db1, "db1", "travelrecord2")
                            .getColumnCount());

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n ADD COLUMN user_id varchar(30);");

            Assert.assertEquals(2,
                    getColumns(db1, "db1", "travelrecord2")
                            .getColumnCount());

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n MODIFY COLUMN user_id varchar(30);");

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n ADD INDEX user_id_idx (user_id);");

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n RENAME INDEX `user_id_idx` TO `iuser_id_idx_new`;");

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n DROP INDEX `iuser_id_idx_new`;");

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n DROP COLUMN user_id;");




            Assert.assertEquals(1,
                    getColumns(db1, "db1", "travelrecord2")
                            .getColumnCount());

        }
    }

    @Test
    public void testGlobal() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection db1 = getMySQLConnection(DB1);
             Connection db2 = getMySQLConnection(DB2)) {
            execute(mycatConnection, RESET_CONFIG);

            JdbcUtils.execute(db1,
                    "drop TABLE if exists db1.`travelrecord2`");
            JdbcUtils.execute(db2,
                    "drop TABLE if exists db1.`travelrecord2`");

            execute(mycatConnection, "DROP DATABASE db1");
            execute(mycatConnection, "CREATE DATABASE db1");


            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));
            execute(mycatConnection, CreateDataSourceHint
                    .create("ds1",
                            DB2));
            execute(mycatConnection, CreateClusterHint.create("c0",
                    Arrays.asList("ds0"), Collections.emptyList()));
            execute(mycatConnection, CreateClusterHint.create("c1",
                    Arrays.asList("ds1"), Collections.emptyList()));

            execute(mycatConnection, "CREATE DATABASE db1");
            execute(mycatConnection, "CREATE TABLE db1.`travelrecord2` (\n" +
                    "  `id` bigint(20) NOT NULL KEY " +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 broadcast\n");

            Assert.assertEquals(1,
                    getColumns(mycatConnection, "db1", "travelrecord2")
                            .getColumnCount());
            Assert.assertEquals(1,
                    getColumns(db1, "db1", "travelrecord2")
                            .getColumnCount());

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n ADD COLUMN user_id varchar(30);");

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n ADD INDEX user_id_idx (user_id);");

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n RENAME INDEX `user_id_idx` TO `iuser_id_idx_new`;");

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n DROP INDEX `iuser_id_idx_new`;");

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n MODIFY COLUMN user_id varchar(30);");

            Assert.assertEquals(2,
                    getColumns(db2, "db1", "travelrecord2")
                            .getColumnCount());

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n DROP COLUMN user_id;");

            Assert.assertEquals(1,
                    getColumns(db1, "db1", "travelrecord2")
                            .getColumnCount());
        }
    }

    @Test
    public void testSharding() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection db1 = getMySQLConnection(DB1);
             Connection db2 = getMySQLConnection(DB2)) {
            execute(mycatConnection, RESET_CONFIG);

            JdbcUtils.execute(db1,
                    "DROP DATABASE if exists db1");
            JdbcUtils.execute(db2,
                    "DROP DATABASE if exists db1");
            JdbcUtils.execute(db1,
                    "DROP DATABASE if exists db1_1");
            JdbcUtils.execute(db2,
                    "DROP DATABASE if exists db1_1");
            JdbcUtils.execute(db1,
                    "DROP DATABASE if exists db1_0");
            JdbcUtils.execute(db2,
                    "DROP DATABASE if exists db1_0");

            execute(mycatConnection, "DROP DATABASE db1");
            execute(mycatConnection, "CREATE DATABASE db1");


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
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 dbpartition by mod_hash(id) tbpartition by mod_hash(id) tbpartitions 2 dbpartitions 2;\n");

            Assert.assertEquals(1,
                    getColumns(mycatConnection, "db1", "travelrecord2")
                            .getColumnCount());

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n ADD COLUMN user_id varchar(30);");

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n ADD INDEX user_id_idx (user_id);");

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n RENAME INDEX `user_id_idx` TO `iuser_id_idx_new`;");

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n DROP INDEX `iuser_id_idx_new`;");

            Assert.assertEquals(2,
                    getColumns(db2, "db1_1", "travelrecord2_3")
                            .getColumnCount());

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n MODIFY COLUMN user_id varchar(30);");

            JdbcUtils.execute(mycatConnection,
                    "ALTER TABLE db1.`travelrecord2`\n DROP COLUMN user_id;");

            Assert.assertEquals(1,
                    getColumns(db2, "db1_1", "travelrecord2_3")
                            .getColumnCount());
        }
    }
}
