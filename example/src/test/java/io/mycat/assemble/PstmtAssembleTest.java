package io.mycat.assemble;

import com.alibaba.druid.pool.DruidDataSource;
import com.mysql.cj.jdbc.MysqlDataSource;
import io.mycat.example.MycatRunner;
import lombok.SneakyThrows;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.function.Function;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class PstmtAssembleTest extends AssembleTest {
    @Override
    public Connection getMySQLConnection(String url) throws Exception {
        if (DB_MYCAT.equals(url)){
            url = DB_MYCAT_PSTMT;
        }
        return super.getMySQLConnection(url);
    }
    @Override
    public void testTranscationFail2() throws Exception {
        super.testTranscationFail2();
    }

    @Override
    public void testTranscationFail() throws Exception {
        super.testTranscationFail();
    }

    @Override
    public void testBase() throws Exception {
        super.testBase();
    }

    @Override
    public void testProxyNormalTranscation() throws Exception {
        super.testProxyNormalTranscation();
    }

    @Override
    public void testXANormalTranscation() throws Exception {
        super.testXANormalTranscation();
    }

    @Override
    public void testInfoFunction() throws Exception {
        super.testInfoFunction();
    }


}
