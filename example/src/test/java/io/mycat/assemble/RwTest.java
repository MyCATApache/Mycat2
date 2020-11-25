package io.mycat.assemble;

import io.mycat.config.GlobalBackEndTableInfoConfig;
import io.mycat.hint.CreateTableHint;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class RwTest extends AssembleTest {


    @Test
    public void testRw() throws SQLException {
        try (Connection mycat = getMySQLConnection(8066);
             Connection readMysql = getMySQLConnection(3306);) {
            String db = "testSchema";
            execute(mycat, "drop database " + db);
            execute(mycat, "create database " + db);
            execute(mycat, "use " + db);

            execute(mycat,
                    "/*+ mycat:addDatasource{\"name\":\"dw0\",\"url\":\"jdbc:mysql://127.0.0.1:3306\",\"user\":\"root\",\"password\":\"123456\"} */;");
            execute(mycat,
                    "/*+ mycat:addDatasource{\"name\":\"dr0\",\"url\":\"jdbc:mysql://127.0.0.1:3307\",\"user\":\"root\",\"password\":\"123456\"} */;");
            execute(mycat,
                    "/*! mycat:addCluster{\"name\":\"c0\",\"masters\":[\"dw0\"],\"replicas\":[\"dr0\"]} */;");

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
}
