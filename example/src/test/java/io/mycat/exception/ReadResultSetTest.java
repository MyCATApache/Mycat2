package io.mycat.exception;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;

public class ReadResultSetTest implements MycatTest {

    @Test(expected = Exception.class)
    public void testNormal() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "CREATE DATABASE db1");
            execute(mycatConnection, "CREATE TABLE db1.`travelrecord2` (\n" +
                    "  `id` bigint(20) NOT NULL KEY " +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n");
            deleteData(mycatConnection,"db1","travelrecord2");
            execute(mycatConnection,"insert db1.`travelrecord2` (id) VALUES (1)");
            executeQuery(mycatConnection, "SELECT fff()  FROM  db1.`travelrecord2`");
        }
    }

    @Test(expected = Exception.class)
    public void testNormalInsert() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "CREATE DATABASE db1");
            execute(mycatConnection, "CREATE TABLE db1.`travelrecord2` (\n" +
                    "  `id` bigint(20) NOT NULL KEY " +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n");
            deleteData(mycatConnection,"db1","travelrecord2");
            execute(mycatConnection,"insert db1.`travelrecord2` (id) VALUES (1)");
            execute(mycatConnection,"insert db1.`travelrecord2` (id) VALUES (1)");
        }
    }

    @Test(expected = Exception.class)
    public void testNormalTranscation() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "CREATE DATABASE db1");
            execute(mycatConnection, "CREATE TABLE db1.`travelrecord2` (\n" +
                    "  `id` bigint(20) NOT NULL KEY " +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n");
            deleteData(mycatConnection,"db1","travelrecord2");
            mycatConnection.setAutoCommit(false);
            execute(mycatConnection,"insert db1.`travelrecord2` (id) VALUES (1)");
            executeQuery(mycatConnection, "SELECT fff()  FROM  db1.`travelrecord2`");
        }
    }

    @Test(expected = Exception.class)
    public void testSharding() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");

            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));
            execute(mycatConnection, CreateDataSourceHint
                    .create("ds1",
                            DB2));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));
            execute(mycatConnection,
                    CreateClusterHint.create("c1",
                            Arrays.asList("ds1"), Collections.emptyList()));

            execute(mycatConnection, "USE `db1`;");

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

            deleteData(mycatConnection,"db1","travelrecord");
            execute(mycatConnection,"insert db1.`travelrecord` (id) VALUES (1)");
            executeQuery(mycatConnection, "SELECT fff()  FROM  db1.`travelrecord`");
        }
    }


    @Test(expected = Exception.class)
    public void testShardingTranscation() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");

            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));
            execute(mycatConnection, CreateDataSourceHint
                    .create("ds1",
                            DB2));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));
            execute(mycatConnection,
                    CreateClusterHint.create("c1",
                            Arrays.asList("ds1"), Collections.emptyList()));

            execute(mycatConnection, "USE `db1`;");

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

            deleteData(mycatConnection,"db1","travelrecord");
            execute(mycatConnection,"insert db1.`travelrecord` (id) VALUES (1)");
            executeQuery(mycatConnection, "SELECT fff()  FROM  db1.`travelrecord`");
        }
    }

    @Test(expected = Exception.class)
    public void testShardingInsert() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");

            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));
            execute(mycatConnection, CreateDataSourceHint
                    .create("ds1",
                            DB2));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));
            execute(mycatConnection,
                    CreateClusterHint.create("c1",
                            Arrays.asList("ds1"), Collections.emptyList()));

            execute(mycatConnection, "USE `db1`;");

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

            deleteData(mycatConnection,"db1","travelrecord");
            execute(mycatConnection,"insert db1.`travelrecord` (id) VALUES (1)");
            execute(mycatConnection,"insert db1.`travelrecord` (id) VALUES (1)");
        }
    }
}
