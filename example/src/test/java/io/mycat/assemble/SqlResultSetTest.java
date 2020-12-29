package io.mycat.assemble;

import io.mycat.config.SqlCacheConfig;
import io.mycat.hint.CreateSqlCacheHint;
import io.mycat.hint.DropSqlCacheHint;
import io.mycat.hint.ShowSqlCacheHint;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class SqlResultSetTest implements MycatTest {
    @Test
    public void testCreateSqlCache() throws Exception {
        try (Connection connection = getMySQLConnection(DB_MYCAT);) {
            execute(connection, RESET_CONFIG);
            SqlCacheConfig sqlCacheConfig = new SqlCacheConfig();

            List<Map<String, Object>> res;
            List<Map<String, Object>> maps = executeQuery(connection, CreateSqlCacheHint.create(sqlCacheConfig));
            Assert.assertEquals(1, maps.size());
            res = executeQuery(connection, ShowSqlCacheHint.create());
            Assert.assertEquals(1, res.size());
            long end = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
            boolean hit = false;
            while (true) {
                if (System.currentTimeMillis() > end) {
                    break;
                }
                res = executeQuery(connection, ShowSqlCacheHint.create());
                hit = res.iterator().next().toString().contains("hasCache:true");
                if (hit){
                    break;
                }
            }
            Assert.assertTrue(hit);
            execute(connection, DropSqlCacheHint.create(sqlCacheConfig.getName()));

            Assert.assertEquals(0, executeQuery(connection, ShowSqlCacheHint.create()).size());
        }
    }
}
