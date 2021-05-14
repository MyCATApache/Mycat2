package io.mycat.hint;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.Collections;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class HintTest implements MycatTest {

    @Test
    public void testTimeout() throws Exception {
        try(Connection mySQLConnection = getMySQLConnection(DB_MYCAT);){
            JdbcUtils.executeQuery(mySQLConnection,"/*+MYCAT:EXECUTE_TIMEOUT(time)*/ select sleep(1)", Collections.emptyList());
        }
    }
}