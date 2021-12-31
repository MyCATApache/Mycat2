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

        try (Connection one = getMySQLConnection(DB_MYCAT);
             Connection two = getMySQLConnection(DB_MYCAT);) {
            //base
            {
                Assert.assertTrue(executeQuery(one,
                        "SELECT GET_LOCK('1',0) as v;").toString().contains("1"));
                Assert.assertTrue(executeQuery(one,
                        "SELECT RELEASE_LOCK('1')  as v;").toString().contains("1"));
                Assert.assertTrue(executeQuery(one, "SELECT IS_FREE_LOCK('1') as v;").toString().contains("1"));
            }

            {
                Assert.assertTrue(executeQuery(one,
                        "SELECT GET_LOCK('1',0) as v;").toString().contains("1"));
                Assert.assertTrue(executeQuery(one, "SELECT IS_FREE_LOCK('1');").toString().contains("0"));
                Assert.assertTrue(executeQuery(one,
                        "SELECT RELEASE_LOCK('1') as v;").toString().contains("1"));
                Assert.assertTrue(executeQuery(one, "SELECT IS_FREE_LOCK('1');").toString().contains("1"));
            }

            {
                Assert.assertTrue(executeQuery(one,
                        "SELECT GET_LOCK('1',0) as v;").toString().contains("1"));
                Assert.assertTrue(executeQuery(two, "SELECT IS_FREE_LOCK('1');").toString().contains("0"));

                Assert.assertTrue(executeQuery(one,
                        "SELECT RELEASE_LOCK('1') as v;").toString().contains("1"));

                Assert.assertTrue(executeQuery(two, "SELECT IS_FREE_LOCK('1') as v;").toString().contains("1"));
            }

            {
                Assert.assertTrue(executeQuery(one,
                        "SELECT GET_LOCK('1',0) as v;").toString().contains("1"));
                Assert.assertTrue(executeQuery(two,
                        "SELECT GET_LOCK('1',0) as v;").toString().contains("0"));

                Assert.assertTrue(executeQuery(one,
                        "SELECT RELEASE_LOCK('1') as v;").toString().contains("1"));

                Assert.assertTrue(executeQuery(two, "SELECT IS_FREE_LOCK('1') as v;").toString().contains("1"));

                Assert.assertTrue(executeQuery(two,
                        "SELECT GET_LOCK('1',0) as v;").toString().contains("1"));

                Assert.assertTrue(executeQuery(two,
                        "SELECT RELEASE_LOCK('1') as v;").toString().contains("1"));


                Assert.assertTrue(executeQuery(one, "SELECT IS_FREE_LOCK('1') as v;").toString().contains("1"));
            }

            {
                Assert.assertTrue(executeQuery(one,
                        "SELECT GET_LOCK('1',0) as v;").toString().contains("1"));
                Assert.assertTrue(executeQuery(two,
                        "SELECT GET_LOCK('1',1000) as v;").toString().contains("0"));

                Assert.assertTrue(executeQuery(one,
                        "SELECT RELEASE_LOCK('1') as v;").toString().contains("1"));

                Assert.assertTrue(executeQuery(two, "SELECT IS_FREE_LOCK('1') as v;").toString().contains("1"));

                Assert.assertTrue(executeQuery(two,
                        "SELECT GET_LOCK('1',0) as v;").toString().contains("1"));

                Assert.assertTrue(executeQuery(two,
                        "SELECT RELEASE_LOCK('1') as v;").toString().contains("1"));


                Assert.assertTrue(executeQuery(one, "SELECT IS_FREE_LOCK('1') as v;").toString().contains("1"));
            }

        }
    }


}
