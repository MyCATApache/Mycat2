package io.mycat.assemble;

import io.mycat.hint.CreateDataSourceHint;
import io.mycat.hint.CreateTableHint;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class SetTranscationTest implements MycatTest {

    @Test
    @SneakyThrows
    public void test() {
        try (Connection mycat = getMySQLConnection(DB_MYCAT);) {
            execute(mycat, RESET_CONFIG);
            List<Map<String, Object>> maps;
            execute(mycat, "SET SESSION TRANSACTION READ WRITE");
            maps = executeQuery(mycat, " SELECT @@session.transaction_read_only");
            Assert.assertTrue(maps.toString().contains("0"));
            execute(mycat, "SET SESSION TRANSACTION READ only");
            maps = executeQuery(mycat, " SELECT @@session.transaction_read_only");
            Assert.assertTrue(maps.toString().contains("1"));
            execute(mycat, "SET SESSION TRANSACTION READ WRITE");
            maps = executeQuery(mycat, " SELECT @@session.transaction_read_only");
            Assert.assertTrue(maps.toString().contains("0"));
        }
    }

}