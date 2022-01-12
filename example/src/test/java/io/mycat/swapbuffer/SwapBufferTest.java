package io.mycat.swapbuffer;

import io.mycat.assemble.MycatTest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class SwapBufferTest implements MycatTest {
    @Test
    public void test() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, "/*+mycat:setDebug{1}*/");
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "/*+mycat:is{debug}*/");
            Assert.assertEquals("[{value=1}]",maps.toString());
            List<Map<String, Object>> maps2 = executeQuery(mycatConnection, "SELECT swapbuffer");
            if (maps2.isEmpty()){

            }else {
                Assert.assertEquals("[{1=1}, {1=3}]",maps2.toString());
            }
            execute(mycatConnection, "/*+mycat:setDebug{0}*/");
            System.out.println();
        }
    }
    @Test
    public void test2() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT)) {
            execute(mycatConnection, "/*+mycat:setDebug{1}*/");
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "/*+mycat:is{debug}*/");
            Assert.assertEquals("[{value=1}]",maps.toString());
            List<Map<String, Object>> maps2 = executeQuery(mycatConnection, "SELECT arrow");
            Assert.assertEquals("[{1=1}, {1=3}]",maps2.toString());
            execute(mycatConnection, "/*+mycat:setDebug{0}*/");
            System.out.println();
        }
    }
}
