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
            execute(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint(20) NOT NULL KEY " +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 dbpartition by mod_hash(id) tbpartition by mod_hash(id) tbpartitions 2 dbpartitions 2;\n");
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "/*+mycat:vector() */SELECT * FROM `db1`.`travelrecord` LIMIT 0, 1000; ");

        }
    }

}
