package io.mycat.connection;

import com.alibaba.druid.util.JdbcUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.jupiter.api.Disabled;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
@Disabled
@Ignore
public class ProxyBackendConnectionTest  extends DefaultBackendConnectionTest{
    private boolean init = false;

    @Before
    public void before() throws Exception {
        if (!init) {
            try (Connection connection = getMySQLConnection(DB_MYCAT)) {
                JdbcUtils.execute(connection, "/*+ mycat:readXARecoveryLog{} */;");
            }
            init = true;
        }
    }
    @Override
    public Connection getMySQLConnection(String url) throws Exception {
        Connection mySQLConnection = super.getMySQLConnection(url);
        JdbcUtils.execute(mySQLConnection,"set transaction_policy = 'proxy'");
        return mySQLConnection;
    }
}
