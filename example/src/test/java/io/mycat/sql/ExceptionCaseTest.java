package io.mycat.sql;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.ShardingBackEndTableInfoConfig;
import io.mycat.config.ShardingFunction;
import io.mycat.config.ShardingTableConfig;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.hint.CreateSchemaHint;
import io.mycat.hint.CreateTableHint;
import io.mycat.router.mycat1xfunction.PartitionByFileMap;
import io.mycat.router.mycat1xfunction.PartitionByHotDate;
import io.mycat.util.ByteUtil;
import io.vertx.core.json.Json;
import org.apache.groovy.util.Maps;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class ExceptionCaseTest implements MycatTest {

    @Test
    public void case2() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, RESET_CONFIG);

            execute(mycatConnection, "DROP DATABASE db1");


            execute(mycatConnection, "CREATE DATABASE db1");


            execute(mycatConnection, CreateDataSourceHint
                    .create("ds0",
                            DB1));

            execute(mycatConnection,
                    CreateClusterHint.create("c0",
                            Arrays.asList("ds0"), Collections.emptyList()));

            execute(mycatConnection, "USE `db1`;");

            execute(mycatConnection, "CREATE TABLE if not exists `company` ( `id` int(11) NOT NULL AUTO_INCREMENT,`companyname` varchar(20) DEFAULT NULL,`addressid` int(11) DEFAULT NULL,PRIMARY KEY (`id`))");

            execute(mycatConnection, "delete from db1.company");

            execute(mycatConnection, "INSERT INTO `db1`.`company` (id,`companyname`,`addressid`) VALUES (1,'Intel',1)");

            try {
                execute(mycatConnection, "INSERT INTO `db1`.`company` (id,`companyname`,`addressid`) VALUES (1,'Intel',1)");
            } catch (SQLException e) {
                Assert.assertEquals(1062,e.getErrorCode());
                Assert.assertEquals("23000",e.getSQLState());
                Assert.assertEquals("Duplicate entry '1' for key 'PRIMARY'",e.getMessage());
                System.out.println();
            }


            execute(mycatConnection, "CREATE TABLE if not exists `company` ( `id` int(11) NOT NULL AUTO_INCREMENT,`companyname` varchar(20) DEFAULT NULL,`addressid` int(11) DEFAULT NULL,PRIMARY KEY (`id`))"
                    + " dbpartition by mod_hash(id) tbpartition by mod_hash(id) tbpartitions 2 dbpartitions 2;");

            execute(mycatConnection, "delete from db1.company");

            execute(mycatConnection, "INSERT INTO `db1`.`company` (id,`companyname`,`addressid`) VALUES (1,'Intel',1)");

            try {
                execute(mycatConnection, "INSERT INTO `db1`.`company` (id,`companyname`,`addressid`) VALUES (1,'Intel',1)");
            } catch (SQLException e) {
                Assert.assertEquals(1062,e.getErrorCode());
                Assert.assertEquals("23000",e.getSQLState());
                Assert.assertEquals("Duplicate entry '1' for key 'PRIMARY'",e.getMessage());
                System.out.println();
            }
        }
    }

}
