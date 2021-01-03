package io.mycat.assemble;

import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class CharsetTest implements MycatTest {
    @Test
    public void testChineseCharset() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
        ) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "create database db1");
            execute(mycatConnection, "use db1");
            execute(mycatConnection, "CREATE TABLE `travelrecord` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");
            deleteData(mycatConnection, "db1", "travelrecord");
            Assert.assertTrue(!hasData(mycatConnection, "db1", "travelrecord"));
            try (PreparedStatement preparedStatement = mycatConnection.prepareStatement("insert travelrecord (id,user_id) VALUES (?,?)")) {
                preparedStatement.setInt(1, 1);
                preparedStatement.setString(2, "中文");
                preparedStatement.executeUpdate();
            }

            List<Map<String, Object>> maps = executeQuery(mycatConnection, "select * from travelrecord");
            Assert.assertEquals(maps.size(), 1);
            Assert.assertEquals(maps.get(0).get("user_id"), "中文");

            deleteData(mycatConnection, "db1", "travelrecord");
            Assert.assertTrue(!hasData(mycatConnection, "db1", "travelrecord"));

            execute(mycatConnection,
                    CreateDataSourceHint
                            .create("dw0",
                                    DB2));

            execute(mycatConnection,
                    CreateClusterHint
                            .create("c0",
                                    Arrays.asList("dw0"), Collections.emptyList()));

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
                    + " dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2 dbpartitions 2;");

            deleteData(mycatConnection, "db1", "travelrecord");
            Assert.assertTrue(!hasData(mycatConnection, "db1", "travelrecord"));

            try (PreparedStatement preparedStatement = mycatConnection.prepareStatement("insert travelrecord (id,user_id) VALUES (?,?)")) {
                preparedStatement.setInt(1, 1);
                preparedStatement.setString(2, "中文");
                preparedStatement.executeUpdate();
            }

            maps = executeQuery(mycatConnection, "select * from travelrecord");
            Assert.assertEquals(maps.size(), 1);
            Assert.assertEquals(maps.get(0).get("user_id"), "中文");

        }
    }
}
