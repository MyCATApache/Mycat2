package io.mycat.assemble;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class LockTest implements MycatTest {


    @Test
    public void testLock() throws Exception {

        try (Connection connection = getMySQLConnection(DB_MYCAT);) {
            //base
            {
                Assert.assertTrue(executeQuery(connection,
                        "SELECT GET_LOCK('1',0);").toString().contains("1"));
                Assert.assertTrue(executeQuery(connection,
                        "SELECT RELEASE_LOCK('1');").toString().contains("1"));
                Assert.assertTrue(executeQuery(connection, "SELECT IS_FREE_LOCK('1');").toString().contains("1"));
            }

            {
                Assert.assertTrue(executeQuery(connection,
                        "SELECT GET_LOCK('1',0);").toString().contains("1"));
                Assert.assertTrue(executeQuery(connection, "SELECT IS_FREE_LOCK('1');").toString().contains("0"));
                Assert.assertTrue(executeQuery(connection,
                        "SELECT RELEASE_LOCK('1');").toString().contains("1"));
                Assert.assertTrue(executeQuery(connection, "SELECT IS_FREE_LOCK('1');").toString().contains("1"));
            }
         }
    }


}
