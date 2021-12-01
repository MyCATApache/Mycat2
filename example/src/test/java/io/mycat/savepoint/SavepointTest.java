package io.mycat.savepoint;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.prototypeserver.mysql.PrototypeService;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Arrays;
import java.util.function.Consumer;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class SavepointTest implements MycatTest {
    boolean init = false;
    private static final Logger LOGGER = LoggerFactory.getLogger(SavepointTest.class);
    @Before
    public void before() throws Exception {
        if (!init) {
            try (Connection connection = getMySQLConnection(DB_MYCAT)) {
                JdbcUtils.execute(connection, "/*+ mycat:readXARecoveryLog{} */;");
            }
            init = true;
        }
    }

    @Test
    public void testSavepoint() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);
            initCluster(mycatConnection);
            execute(mycatConnection, "CREATE DATABASE db1");
            execute(mycatConnection, "CREATE TABLE db1.`travelrecord` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `user_id` varchar(100) DEFAULT NULL,\n" +
                    "  `traveldate` date DEFAULT NULL,\n" +
                    "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                    "  `days` int DEFAULT NULL,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");
            execute(mycatConnection, "use db1");
            {
                /*
                savepoint x;
                release x;
                rollback x;
                rollback;
                 */
                deleteData(mycatConnection, "db1", "travelrecord");
                mycatConnection.setAutoCommit(false);
                execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('9999999999');");
                Savepoint savepoint = mycatConnection.setSavepoint();
                execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('1');");
                execute(mycatConnection,"RELEASE SAVEPOINT "+savepoint.getSavepointName());
                try {
                    mycatConnection.rollback(savepoint);
                }catch (SQLException exception){
                    Assert.assertTrue(exception.getMessage().contains("SAVEPOINT"));
                }


                mycatConnection.rollback();
                mycatConnection.setAutoCommit(true);
                long count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count ==0);
                mycatConnection.setAutoCommit(true);
            }
            {
                /*
                savepoint x;
                rollback x;
                release x;
                rollback;
                 */
                deleteData(mycatConnection, "db1", "travelrecord");
                mycatConnection.setAutoCommit(false);
                execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('9999999999');");
                Savepoint savepoint = mycatConnection.setSavepoint();
                execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('1');");
                mycatConnection.rollback(savepoint);
                try {
                    execute(mycatConnection,"RELEASE SAVEPOINT "+savepoint.getSavepointName());
                }catch (SQLException exception){
                    System.out.println("Savepoint fail :"+exception);
                    LOGGER.error("Savepoint fail :"+exception);
                    Assert.assertTrue(exception.getMessage().contains("SAVEPOINT"));
                }


                mycatConnection.rollback();
                mycatConnection.setAutoCommit(true);
                long count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count ==0);
                mycatConnection.setAutoCommit(true);
            }
            {
                /*
                savepoint x;
                savepoint x;
                commit;
                 */
                long count;
                deleteData(mycatConnection, "db1", "travelrecord");
                mycatConnection.setAutoCommit(false);
                execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('9999999999');");
                count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count==1);
                Savepoint savepoint = mycatConnection.setSavepoint();
                execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('1');");
                count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count==2);
                 savepoint = mycatConnection.setSavepoint(savepoint.getSavepointName());
                count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count==2);
                mycatConnection.commit();
                count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count==2);
                mycatConnection.setAutoCommit(true);
            }
            {
                /*
                savepoint x;
                savepoint x;
                rollback;
                 */
                long count;
                deleteData(mycatConnection, "db1", "travelrecord");
                mycatConnection.setAutoCommit(false);
                execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('9999999999');");
                count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count==1);
                Savepoint savepoint = mycatConnection.setSavepoint();
                execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('1');");
                count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count==2);
                savepoint = mycatConnection.setSavepoint(savepoint.getSavepointName());
                count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count==2);
                mycatConnection.rollback();
                count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count==0);
                mycatConnection.setAutoCommit(true);
            }
            {
                /*
                savepoint x;
                rollback x;
                rollback;
                 */
                long count;
                deleteData(mycatConnection, "db1", "travelrecord");
                mycatConnection.setAutoCommit(false);
                execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('9999999999');");
                count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count==1);
                Savepoint savepoint = mycatConnection.setSavepoint();
                execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('1');");
                count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count==2);
                mycatConnection.rollback(savepoint);
                count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count==1);
                mycatConnection.rollback();
                mycatConnection.setAutoCommit(true);
                count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count==0);
                mycatConnection.setAutoCommit(true);
            }
            {
                /*
                savepoint x;
                rollback x;
                commit;
                 */
                long count;
                deleteData(mycatConnection, "db1", "travelrecord");
                mycatConnection.setAutoCommit(false);
                execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('9999999999');");
                count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count==1);
                Savepoint savepoint = mycatConnection.setSavepoint();
                execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('1');");
                count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count==2);
                mycatConnection.rollback(savepoint);
                count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count==1);
                mycatConnection.commit();
                mycatConnection.setAutoCommit(true);
                count = count(mycatConnection, "db1", "travelrecord");
                Assert.assertTrue(count==1);
                mycatConnection.setAutoCommit(true);
            }
            {
                /*
                savepoint x;
                commit;
                 */
                deleteData(mycatConnection, "db1", "travelrecord");
                mycatConnection.setAutoCommit(false);
                execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('9999999999');");
                Savepoint savepoint = mycatConnection.setSavepoint();
                mycatConnection.commit();
                mycatConnection.setAutoCommit(true);
                Assert.assertTrue(count(mycatConnection, "db1", "travelrecord") == 1);
                mycatConnection.setAutoCommit(true);
            }
            {
                /*
                savepoint x;
                release x;
                commit;
                 */
                deleteData(mycatConnection, "db1", "travelrecord");
                mycatConnection.setAutoCommit(false);
                execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('9999999999');");
                Savepoint savepoint = mycatConnection.setSavepoint();
                execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('1');");
                mycatConnection.releaseSavepoint(savepoint);
                mycatConnection.commit();
                mycatConnection.setAutoCommit(true);
                Assert.assertTrue(count(mycatConnection, "db1", "travelrecord") ==2);
                mycatConnection.setAutoCommit(true);
            }

            System.out.println();
        }
    }

    @Test
    public void testTranscationFail2() throws Exception {
        Consumer<Connection> connectionFunction = new Consumer<Connection>() {

            @SneakyThrows
            @Override
            public void accept(Connection mycatConnection) {
                execute(mycatConnection, "set transaction_policy  = proxy");
            }
        };

        testTranscation(connectionFunction);
    }

    @Test
    public void testTranscationFail() throws Exception {
        Consumer<Connection> connectionFunction = new Consumer<Connection>() {

            @SneakyThrows
            @Override
            public void accept(Connection mycatConnection) {
                SavepointTest.this.execute(mycatConnection, "set transaction_policy  = xa");
            }
        };
        testTranscation(connectionFunction);
    }

    private void testTranscation(Consumer<Connection> connectionFunction) throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            execute(mycatConnection, RESET_CONFIG);
            initCluster(mycatConnection);
            connectionFunction.accept(mycatConnection);
            execute(mycatConnection, "CREATE DATABASE db1");
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
            execute(mycatConnection, "use db1");
            deleteData(mycatConnection, "db1", "travelrecord");
            mycatConnection.setAutoCommit(false);
            execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('9999999999');");
            execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('1');");
            execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`,`user_id`) VALUES ('1',999/0);");
        } catch (Throwable ignored) {

        }
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            connectionFunction.accept(mycatConnection);
            execute(mycatConnection, "use db1");
            Assert.assertTrue(executeQuery(mycatConnection,
                    "select *  from travelrecord"
            ).isEmpty());
            mycatConnection.setAutoCommit(false);
            execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('9999999999');");
            execute(mycatConnection, "INSERT INTO `db1`.`travelrecord` (`id`) VALUES ('1');");
            mycatConnection.commit();
            Assert.assertTrue(hasData(mycatConnection, "db1", "travelrecord"));
        }
    }

    @Test
    public void testProxyNormalTranscation() throws Exception {
        try (Connection mySQLConnection = getMySQLConnection(DB_MYCAT);) {
            testNormalTranscationWrapper(mySQLConnection, "set transaction_policy = proxy", "proxy");
        }
    }

    @Test
    public void testXANormalTranscation() throws Exception {
        try (Connection mySQLConnection = getMySQLConnection(DB_MYCAT);) {
            testNormalTranscationWrapper(mySQLConnection, "set transaction_policy = xa", "xa");

        }
    }

    private void testNormalTranscationWrapper(Connection mycatConnection, String s, String proxy) throws Exception {
        //////////////////////////////////////transcation/////////////////////////////////////////////

        execute(mycatConnection, s);
        Assert.assertTrue(executeQuery(mycatConnection, "select @@transaction_policy").toString().contains(proxy));

        testProxyNormalTranscation(mycatConnection);
    }

    protected void initCluster(Connection mycatConnection) throws Exception {
        execute(mycatConnection,
                CreateDataSourceHint
                        .create("dw0", DB1));

        execute(mycatConnection,
                CreateDataSourceHint
                        .create("dr0", DB1));

        execute(mycatConnection,
                CreateDataSourceHint
                        .create("dw1", DB2));

        execute(mycatConnection,
                CreateDataSourceHint
                        .create("dr1", DB2));

        execute(mycatConnection,
                CreateClusterHint
                        .create("c0",
                                Arrays.asList("dw0"), Arrays.asList("dr0")));

        execute(mycatConnection,
                CreateClusterHint
                        .create("c1",
                                Arrays.asList("dw1"), Arrays.asList("dr1")));
    }


    private void testProxyNormalTranscation(Connection mycatConnection) throws Exception {
        execute(mycatConnection, RESET_CONFIG);
        addC0(mycatConnection);
        execute(mycatConnection, "CREATE DATABASE db1");
        execute(mycatConnection, "use db1");
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

        deleteData(mycatConnection, "db1", "travelrecord");
        mycatConnection.setAutoCommit(false);
        Assert.assertTrue(
                executeQuery(mycatConnection, "SELECT @@autocommit;").toString().contains("0")
        );
        execute(mycatConnection,
                "insert  into `travelrecord`(`id`,`user_id`) values (1,'999'),(999999999,'999');");
        mycatConnection.rollback();

        mycatConnection.setAutoCommit(true);
        Assert.assertTrue(
                executeQuery(mycatConnection, "SELECT @@autocommit;").toString().contains("1")
        );
        Assert.assertFalse(hasData(mycatConnection, "db1", "travelrecord"));


        ///////////////////////////////////////////////////////////////////////////////////////
        mycatConnection.setAutoCommit(false);
        Assert.assertTrue(
                executeQuery(mycatConnection, "SELECT @@autocommit;").toString().contains("0")
        );
        execute(mycatConnection,
                "insert  into `travelrecord`(`id`,`user_id`) values (1,'999'),(999999999,'999');");
        mycatConnection.commit();

        mycatConnection.setAutoCommit(true);
        Assert.assertTrue(
                executeQuery(mycatConnection, "SELECT @@autocommit;").toString().contains("1")
        );
        Assert.assertEquals(2, executeQuery(mycatConnection, "select id from db1.travelrecord").size());
    }


}
