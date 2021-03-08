package io.mycat.connection;

import com.alibaba.druid.util.JdbcUtils;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class XaBackendConnectionTest extends DefaultBackendConnectionTest{
    @Override
    public Connection getMySQLConnection(String url) throws Exception {
        Connection mySQLConnection = super.getMySQLConnection(url);
        JdbcUtils.execute(mySQLConnection,"set transaction_policy = 'xa'");
        return mySQLConnection;
    }
}
