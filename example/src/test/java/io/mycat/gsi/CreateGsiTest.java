package io.mycat.gsi;

import io.mycat.assemble.MycatTest;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class CreateGsiTest implements MycatTest {

    @Test
    public void createGsi() throws Exception{
        initShardingTable();
        try(Connection connection = getMySQLConnection(DB_MYCAT)){
           execute(connection,"CREATE UNIQUE GLOBAL INDEX `g_i_user_id` ON `db1`.`travelrecord`(`user_id`) \n" +
                    "    COVERING(`fee`) \n" +
                    "    dbpartition by hash(`user_id`) tbpartition by hash(`user_id`) dbpartitions 2 tbpartitions 2");
            System.out.println();
        }
    }


    private void initShardingTable() throws Exception {
        Connection mycatConnection = getMySQLConnection(DB_MYCAT);
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
        execute(mycatConnection, "CREATE TABLE `company` ( `id` int(11) NOT NULL AUTO_INCREMENT,`companyname` varchar(20) DEFAULT NULL,`addressid` int(11) DEFAULT NULL,PRIMARY KEY (`id`))");
          execute(mycatConnection, "delete from db1.travelrecord");

        for (int i = 1; i < 10; i++) {
            execute(mycatConnection, "insert db1.travelrecord (id) values(" + i + ")");
        }

        mycatConnection.close();
    }


}
