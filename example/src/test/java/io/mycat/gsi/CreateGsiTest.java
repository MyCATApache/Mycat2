package io.mycat.gsi;

import io.mycat.assemble.MycatTest;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.hint.ShowTopologyHint;
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
public class CreateGsiTest implements MycatTest {

    @Test
    public void createGsi() throws Exception{
        initShardingTable();
        try(Connection connection = getMySQLConnection(DB_MYCAT)){
           execute(connection,"CREATE UNIQUE GLOBAL INDEX `g_i_user_id` ON `db1`.`travelrecord`(`user_id`) \n" +
                    "    COVERING(`fee`,id) \n" +
                    "    dbpartition by mod_hash(`user_id`) tbpartition by mod_hash(`user_id`) dbpartitions 2 tbpartitions 2");
//            boolean b = hasData(connection, "db1", "travelrecord_g_i_user_id");//test create it
//            List<Map<String, Object>> maps = executeQuery(connection, ShowTopologyHint.create("db1", "travelrecord_g_i_user_id").toString());
//            Assert.assertEquals("[{targetName=c0, schemaName=db1_0, tableName=travelrecord_g_i_user_id_0}, {targetName=c0, schemaName=db1_0, tableName=travelrecord_g_i_user_id_1}, {targetName=c1, schemaName=db1_1, tableName=travelrecord_g_i_user_id_2}, {targetName=c1, schemaName=db1_1, tableName=travelrecord_g_i_user_id_3}]",maps.toString());


//            List<Map<String, Object>> maps1 = executeQuery(connection, "select * from db1.travelrecord where id = 1");
//            List<Map<String, Object>> maps2 = executeQuery(connection, "select * from db1.travelrecord where user_id = 1");


//            String explainPrimaryTable = explain(connection, "select * from db1.travelrecord where id = 1");
//            String explainIndexScan = explain(connection, "select * from db1.travelrecord where user_id = 1");//index-scan
     String explainOnlyIndexScan = explain(connection, "select fee from db1.travelrecord where user_id = 1");//index-scan
deleteData(connection,"db1","travelrecord");
            deleteData(connection,"db1","travelrecord_g_i_user_id");
            for (int i = 1; i < 10; i++) {
                execute(connection, "insert db1.travelrecord (id,user_id) values(" + i + "," +
                        "" +
                        i +
                        ")");
            }
            List<Map<String, Object>> maps = executeQuery(connection, "select fee from db1.travelrecord where user_id = 1");

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
        //execute(mycatConnection, "CREATE TABLE `company` ( `id` int(11) NOT NULL AUTO_INCREMENT,`companyname` varchar(20) DEFAULT NULL,`addressid` int(11) DEFAULT NULL,PRIMARY KEY (`id`))");
          execute(mycatConnection, "delete from db1.travelrecord");

        for (int i = 1; i < 10; i++) {
            execute(mycatConnection, "insert db1.travelrecord (id) values(" + i + ")");
        }

        mycatConnection.close();
    }


}
