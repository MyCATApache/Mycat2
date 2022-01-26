package io.mycat.vector;

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
import java.util.List;
import java.util.Map;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class VectorTest implements MycatTest {
    @Test
    public void testBase() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection, "CREATE DATABASE db1");

            execute(mycatConnection, "CREATE TABLE db1.`vector` (\n" +
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

            deleteData(mycatConnection, "db1", "vector");
            for (int i = 1; i < 10; i++) {
                execute(mycatConnection, "insert db1.vector (id,user_id) values(" + i + "," + i%4+")");
            }
            //test hint
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "/*+mycat:vector() */SELECT id FROM `db1`.`vector` LIMIT 0, 1000; ");

            Assert.assertEquals("[{id=4}, {id=8}, {id=1}, {id=5}, {id=9}, {id=2}, {id=6}, {id=3}, {id=7}]",maps.toString());
            try {
                execute(mycatConnection, "/*+mycat:setVector{1}*/");

                maps = executeQuery(mycatConnection, "SELECT count(*) FROM `db1`.`vector` LIMIT 0, 1000; ");

                Assert.assertEquals("[{count(*)=9}]",maps.toString());

                maps = executeQuery(mycatConnection, "SELECT max(id) FROM `db1`.`vector` LIMIT 0, 1000; ");

                Assert.assertEquals("[{max(id)=9}]",maps.toString());

                maps = executeQuery(mycatConnection, "SELECT min(id) FROM `db1`.`vector` LIMIT 0, 1000; ");

                Assert.assertEquals("[{min(id)=1}]",maps.toString());

                maps = executeQuery(mycatConnection, "SELECT sum(id) FROM `db1`.`vector` LIMIT 0, 1000; ");

                Assert.assertEquals("[{sum(id)=45}]",maps.toString());

                maps = executeQuery(mycatConnection, "SELECT avg(id) FROM `db1`.`vector` LIMIT 0, 1000; ");

                Assert.assertEquals("[{avg(id)=5.0}]",maps.toString());

                maps = executeQuery(mycatConnection, "SELECT user_id FROM `db1`.`vector` group by user_id LIMIT 0, 1000; ");

                Assert.assertEquals("[{user_id=0}, {user_id=1}, {user_id=2}, {user_id=3}]",maps.toString());

                maps = executeQuery(mycatConnection, "SELECT max(id) FROM `db1`.`vector` group by user_id LIMIT 0, 1000; ");

                Assert.assertEquals("[{max(id)=8}, {max(id)=9}, {max(id)=6}, {max(id)=7}]",maps.toString());

                System.out.println();
            } finally {
                execute(mycatConnection, "/*+mycat:setVector{0}*/");
            }
        }
    }

}
