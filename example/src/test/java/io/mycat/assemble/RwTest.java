package io.mycat.assemble;

import io.mycat.config.GlobalBackEndTableInfoConfig;
import io.mycat.config.ShardingBackEndTableInfoConfig;
import io.mycat.config.ShardingFuntion;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.hint.CreateTableHint;
import io.mycat.router.mycat1xfunction.PartitionByRangeMod;
import org.apache.groovy.util.Maps;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.sql.Connection;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class RwTest implements MycatTest {


    @Test
    public void testRw() throws Exception {
        try (Connection mycat = getMySQLConnection(8066);
             Connection readMysql = getMySQLConnection(3306);) {
            execute(mycat,RESET_CONFIG);
            String db = "testSchema";
            execute(mycat, "drop database " + db);
            execute(mycat, "create database " + db);
            execute(mycat, "use " + db);

            execute(mycat,
                    "/*+ mycat:createDatasource{\"name\":\"dw0\",\"url\":\"jdbc:mysql://127.0.0.1:3306\",\"user\":\"root\",\"password\":\"123456\"} */;");
            execute(mycat,
                    "/*+ mycat:createDatasource{\"name\":\"dr0\",\"url\":\"jdbc:mysql://127.0.0.1:3307\",\"user\":\"root\",\"password\":\"123456\"} */;");
            execute(mycat,
                    "/*+ mycat:createCluster{\"name\":\"c0\",\"masters\":[\"dw0\"],\"replicas\":[\"dr0\"]} */;");

            execute(readMysql, "drop table if exists " + db + ".normal");
            execute(
                    mycat,
                    CreateTableHint
                            .createNormal(db, "normal", "create table normal(id int)", "c0")
            );
            execute(readMysql, "insert " + db + ".normal (id) VALUES (1)");
            long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
            boolean res = false;
            while (true) {
                res |= hasData(mycat, db, "normal");
                if (res||System.currentTimeMillis() > endTime) {
                    break;
                }
            }
            Assert.assertTrue(res);

            execute(
                    mycat,
                    CreateTableHint
                            .createGlobal(db, "global", "create table global(id int)", Arrays.asList(
                                    GlobalBackEndTableInfoConfig.builder().targetName("prototype").build()))
            );
            execute(mycat, "drop database " + db);
        }
    }


    @Test
    public void testSharding() throws Exception {
        try (Connection mycat = getMySQLConnection(8066);
             Connection prototypeMysql = getMySQLConnection(3306);) {
            execute(mycat,RESET_CONFIG);
            String db = "testSchema";
            String tableName = "sharding";
            execute(mycat, "drop database if EXISTS " + db);
            execute(mycat, "create database " + db);
            execute(mycat, "use " + db);

            execute(mycat, CreateDataSourceHint
                    .create("dw0",
                            "jdbc:mysql://127.0.0.1:3306"));
            execute(mycat, CreateDataSourceHint
                    .create("dw1",
                            "jdbc:mysql://127.0.0.1:3306"));

            execute(prototypeMysql,"use mysql");
            execute(prototypeMysql,"DROP TABLE IF EXISTS MYCAT_SEQUENCE;");
            execute(prototypeMysql,"CREATE TABLE MYCAT_SEQUENCE (  NAME VARCHAR(64) NOT NULL,  current_value BIGINT(20) NOT NULL,  increment INT NOT NULL DEFAULT 1, PRIMARY KEY (NAME) ) ENGINE=INNODB;\n");
            execute(prototypeMysql,"DROP FUNCTION IF EXISTS `mycat_seq_nextval`;");
            execute(prototypeMysql,"CREATE FUNCTION `mycat_seq_nextval`(seq_name VARCHAR(64)) RETURNS VARCHAR(64) CHARSET latin1 DETERMINISTIC   BEGIN     DECLARE retval VARCHAR(64);     DECLARE val BIGINT;     DECLARE inc INT;     DECLARE seq_lock INT;     SET val = -1;     SET inc = 0;    SET seq_lock = -1;     SELECT GET_LOCK(seq_name, 15) INTO seq_lock;     IF seq_lock = 1 THEN      SELECT current_value + increment, increment INTO val, inc FROM MYCAT_SEQUENCE WHERE NAME = seq_name FOR UPDATE;      IF val != -1 THEN        UPDATE MYCAT_SEQUENCE SET current_value = val WHERE NAME = seq_name;      END IF;      SELECT RELEASE_LOCK(seq_name) INTO seq_lock;     END IF;     SELECT CONCAT(CAST((val - inc + 1) AS CHAR),',',CAST(inc AS CHAR)) INTO retval;     RETURN retval;   END");
            execute(prototypeMysql,"INSERT INTO `MYCAT_SEQUENCE` (`name`, `current_value`) VALUES ('"+db+"_"+tableName+"', '0');");


            execute(
                    mycat,
                    CreateTableHint
                            .createSharding(db, tableName,
                                    "create table "+tableName+"(\n" +
                                            "id int(11) NOT NULL AUTO_INCREMENT,\n" +
                                            "user_id int(11) ,\n" +
                                            "user_name varchar(128), \n" +
                                            "PRIMARY KEY (`id`), \n" +
                                            " GLOBAL INDEX `g_i_user_id`(`user_id`) COVERING (`user_name`) dbpartition by btree(`user_id`) \n" +
                                            ")ENGINE=InnoDB DEFAULT CHARSET=utf8 ",
                                    ShardingBackEndTableInfoConfig.builder()
                                            .schemaNames(db)
                                            .tableNames(tableName)
                                            .targetNames("dw0,dw1").build(),
                                    ShardingFuntion.builder()
                                            .clazz(PartitionByRangeMod.class.getCanonicalName())
                                            .properties(Maps.of(
                                                    "defaultNode", -1,
                                                    "columnName","id"))
                                            .ranges(Maps.of(
                                                    "0-1",1,
                                                    "1-300",2)).build())
            );
            execute(mycat,
                    "/*+ mycat:setSequence{\"name\":\""+db+"_"+tableName+"\",\"clazz\":\"io.mycat.plug.sequence.SequenceMySQLGenerator\",\"schemaName\":\"mysql\"} */;");

            execute(mycat, "insert into "+tableName+" (user_id,user_name) VALUES (1,'wang')");
            execute(mycat, "insert into "+tableName+"(user_id,user_name) VALUES (2,'zhang')");
            execute(mycat, "insert into "+tableName+" (user_id,user_name) VALUES (3,'li')");
            long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
            boolean res = false;
            while (true) {
                res |= hasData(mycat, db, tableName);
                if (res||System.currentTimeMillis() > endTime) {
                    break;
                }
            }
            Assert.assertTrue(res);

//            execute(mycat, "drop database " + db);
        }
    }

    private String getPkIncrFunctionSql() throws IOException {
        String sql =
                "CREATE FUNCTION `mycat_seq_nextval`(seq_name VARCHAR(64)) RETURNS VARCHAR(64) CHARSET latin1\n" +
                "DETERMINISTIC\n" +
                "BEGIN\n" +
                "        DECLARE retval VARCHAR(64);\n" +
                "        DECLARE val BIGINT;\n" +
                "        DECLARE inc INT;\n" +
                "        DECLARE seq_lock INT;\n" +
                "        SET val = -1;\n" +
                "        SET inc = 0;\n" +
                "        SET seq_lock = -1;\n" +
                "        SELECT GET_LOCK(seq_name, 15) INTO seq_lock;\n" +
                "        IF seq_lock = 1 THEN\n" +
                "            SELECT current_value + increment, increment INTO val, inc FROM MYCAT_SEQUENCE WHERE NAME = seq_name FOR UPDATE;\n" +
                "            IF val != -1 THEN\n" +
                "                    UPDATE MYCAT_SEQUENCE SET current_value = val WHERE NAME = seq_name;\n" +
                "            END IF;\n" +
                "            SELECT RELEASE_LOCK(seq_name) INTO seq_lock;\n" +
                "        END IF;\n" +
                "        SELECT CONCAT(CAST((val - inc + 1) AS CHAR),',',CAST(inc AS CHAR)) INTO retval;\n" +
                "        RETURN retval;\n" +
                "END";
        return sql;
    }
}
