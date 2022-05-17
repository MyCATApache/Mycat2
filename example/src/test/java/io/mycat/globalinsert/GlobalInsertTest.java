package io.mycat.globalinsert;

import io.mycat.assemble.MycatTest;
import io.mycat.config.GlobalBackEndTableInfoConfig;
import io.mycat.config.GlobalTableConfig;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.hint.CreateTableHint;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class GlobalInsertTest implements MycatTest {

    @Test
    public void testGlobal_NO_SEQUENCE() throws Exception {
        GlobalTableConfig.GlobalTableSequenceType globalTableSequenceType = GlobalTableConfig.GlobalTableSequenceType.NO_SEQUENCE;
        innerTest(globalTableSequenceType);
    }

    @Test
    public void testGlobal_GLOBAL_SEQUENCE() throws Exception {
        GlobalTableConfig.GlobalTableSequenceType globalTableSequenceType = GlobalTableConfig.GlobalTableSequenceType.GLOBAL_SEQUENCE;
        innerTest(globalTableSequenceType);
    }

    @Test
    public void testGlobal_FIRST_SEQUENCE() throws Exception {
        GlobalTableConfig.GlobalTableSequenceType globalTableSequenceType = GlobalTableConfig.GlobalTableSequenceType.FIRST_SEQUENCE;
        innerTest(globalTableSequenceType);
    }

    private void innerTest(GlobalTableConfig.GlobalTableSequenceType globalTableSequenceType) throws Exception {
        try (Connection mycat = getMySQLConnection(DB_MYCAT);
             Connection c0 = getMySQLConnection(DB1);
             Connection c1 = getMySQLConnection(DB2);) {
            List<Connection> cList = Arrays.asList(c0, c1);

            execute(mycat, RESET_CONFIG);

            execute(mycat, CreateDataSourceHint
                    .create("dw0", DB1));
            execute(mycat, CreateDataSourceHint
                    .create("dw1", DB2));

            execute(mycat, CreateClusterHint.create("c0", Arrays.asList("dw0"), Collections.emptyList()));
            execute(mycat, CreateClusterHint.create("c1", Arrays.asList("dw1"), Collections.emptyList()));


            for (Connection connection : cList) {
                execute(connection, "drop database if  exists  " + "db1");
                execute(connection, "create database " + "db1");
                execute(connection, "CREATE TABLE db1.`travelrecord` (\n" +
                        "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                        "  `user_id` varchar(100) DEFAULT NULL,\n" +
                        "  `traveldate` date DEFAULT NULL,\n" +
                        "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                        "  `days` int DEFAULT NULL,\n" +
                        "  `blob` longblob,\n" +
                        "  PRIMARY KEY (`id`),\n" +
                        "  KEY `id` (`id`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8");
                execute(connection, "ALTER TABLE `db1`.`travelrecord` AUTO_INCREMENT=0; ");
            }
            execute(mycat, "create database " + "db1");
            GlobalTableConfig globalTableConfig = new GlobalTableConfig();
            globalTableConfig.setSequenceType(globalTableSequenceType);
            globalTableConfig.setCreateTableSQL("CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8");
            globalTableConfig.setBroadcast(Stream.of("c0", "c1").map(s -> {
                GlobalBackEndTableInfoConfig globalBackEndTableInfoConfig = new GlobalBackEndTableInfoConfig();
                globalBackEndTableInfoConfig.setTargetName(s);
                return globalBackEndTableInfoConfig;
            }).collect(Collectors.toList()));
            execute(mycat, CreateTableHint.createGlobal("db1", "travelrecord", globalTableConfig));

            //////////////////////////////////no-transacation///////////////////////////////////////////////////////////
            {
                execute(mycat, "insert into db1.travelrecord (user_id) VALUES (1)");
                execute(mycat, "insert into db1.travelrecord (user_id) VALUES (2)");
                execute(mycat, "insert into db1.travelrecord (user_id) VALUES (3)");


                List<Map<String, Object>> mycatResult = executeQuery(mycat, "select * from db1.travelrecord");
                List<Map<String, Object>> c0Result = executeQuery(c0, "select * from db1.travelrecord");
                List<Map<String, Object>> c1Result = executeQuery(c1, "select * from db1.travelrecord");

                Assert.assertEquals(mycatResult, c0Result);
                Assert.assertEquals(c0Result, c1Result);

                System.out.println(mycatResult);
            }
            //////////////////////////////////transacation///////////////////////////////////////////////////////////

            {
                mycat.setAutoCommit(false);
                execute(mycat, "insert into db1.travelrecord (user_id) VALUES (1)");
                execute(mycat, "insert into db1.travelrecord (user_id) VALUES (2)");
                execute(mycat, "insert into db1.travelrecord (user_id) VALUES (3)");

                mycat.setAutoCommit(true);
                List<Map<String, Object>> mycatResult = executeQuery(mycat, "select * from db1.travelrecord");
                List<Map<String, Object>> c0Result = executeQuery(c0, "select * from db1.travelrecord");
                List<Map<String, Object>> c1Result = executeQuery(c1, "select * from db1.travelrecord");

                Assert.assertEquals(mycatResult, c0Result);
                Assert.assertEquals(c0Result, c1Result);
            }
            /////////////////////////////////////////////column///////////////////////////////////////////////////////////////
            {
                List<Map<String, Object>> mycatResult;
                List<Map<String, Object>> c0Result;
                List<Map<String, Object>> c1Result;

                execute(mycat, "insert into db1.travelrecord (id,user_id) VALUES (100,1)");
                execute(mycat, "insert into db1.travelrecord (id,user_id) VALUES (101,2)");
                execute(mycat, "insert into db1.travelrecord (id,user_id) VALUES (102,3)");
                execute(mycat, "insert into db1.travelrecord (id,user_id) VALUES (103,4),(104,5)");

                mycatResult = executeQuery(mycat, "select * from db1.travelrecord");
                c0Result = executeQuery(c0, "select * from db1.travelrecord");
                c1Result = executeQuery(c1, "select * from db1.travelrecord");

                Assert.assertEquals(mycatResult, c0Result);
                Assert.assertEquals(c0Result, c1Result);

                execute(mycat, "insert into db1.travelrecord (id,user_id) VALUES (null,1)");
                execute(mycat, "insert into db1.travelrecord (id,user_id) VALUES (null,2)");
                execute(mycat, "insert into db1.travelrecord (id,user_id) VALUES (null,3)");
                execute(mycat, "insert into db1.travelrecord (id,user_id) VALUES (null,4),(null,5)");

                mycatResult = executeQuery(mycat, "select * from db1.travelrecord");
                c0Result = executeQuery(c0, "select * from db1.travelrecord");
                c1Result = executeQuery(c1, "select * from db1.travelrecord");

                Assert.assertEquals(mycatResult, c0Result);
                Assert.assertEquals(c0Result, c1Result);

                execute(mycat, "insert into db1.travelrecord (id,user_id) VALUES (0,1)");
                mycatResult = executeQuery(mycat, "select * from db1.travelrecord");
                c0Result = executeQuery(c0, "select * from db1.travelrecord");
                c1Result = executeQuery(c1, "select * from db1.travelrecord");

                Assert.assertEquals(mycatResult, c0Result);
                Assert.assertEquals(c0Result, c1Result);

                execute(mycat, "insert into db1.travelrecord (id,user_id) VALUES (0,2)");

                mycatResult = executeQuery(mycat, "select * from db1.travelrecord");
                c0Result = executeQuery(c0, "select * from db1.travelrecord");
                c1Result = executeQuery(c1, "select * from db1.travelrecord");

                Assert.assertEquals(mycatResult, c0Result);
                Assert.assertEquals(c0Result, c1Result);

                execute(mycat, "insert into db1.travelrecord (id,user_id) VALUES (0,3)");

                mycatResult = executeQuery(mycat, "select * from db1.travelrecord");
                c0Result = executeQuery(c0, "select * from db1.travelrecord");
                c1Result = executeQuery(c1, "select * from db1.travelrecord");

                Assert.assertEquals(mycatResult, c0Result);
                Assert.assertEquals(c0Result, c1Result);

                execute(mycat, "insert into db1.travelrecord (id,user_id) VALUES (0,4),(0,5)");

                mycatResult = executeQuery(mycat, "select * from db1.travelrecord");
                c0Result = executeQuery(c0, "select * from db1.travelrecord");
                c1Result = executeQuery(c1, "select * from db1.travelrecord");

                Assert.assertEquals(mycatResult, c0Result);
                Assert.assertEquals(c0Result, c1Result);

                System.out.println(mycatResult);
            }
            /////////////////////////////////////////////no-column///////////////////////////////////////////////////////////////
            {
                execute(mycat, "insert into db1.travelrecord (user_id) VALUES (1)");
                execute(mycat, "insert into db1.travelrecord (user_id) VALUES (2)");
                execute(mycat, "insert into db1.travelrecord (user_id) VALUES (3)");


                List<Map<String, Object>> mycatResult = executeQuery(mycat, "select * from db1.travelrecord");
                List<Map<String, Object>> c0Result = executeQuery(c0, "select * from db1.travelrecord");
                List<Map<String, Object>> c1Result = executeQuery(c1, "select * from db1.travelrecord");

                Assert.assertEquals(mycatResult, c0Result);
                Assert.assertEquals(c0Result, c1Result);

                System.out.println(mycatResult);
            }
            System.out.println();
        }
    }


}
