package io.mycat.exception;

import io.mycat.assemble.MycatTest;
import io.mycat.hint.InterruptThreadHint;
import io.mycat.hint.KillThreadHint;
import io.mycat.hint.ShowThreadInfoHint;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public class ThreadTest implements MycatTest {

    @Test
    public void testBase() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            List<Map<String, Object>> maps = executeQuery(mycatConnection, ShowThreadInfoHint.create());
            Assert.assertFalse(maps.isEmpty());
            System.out.println();
        }
    }

    @Test
    @Ignore
    public void testKill() throws Exception {
        List<Map<String, Object>> maps;
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            maps = executeQuery(mycatConnection, ShowThreadInfoHint.create());
            String id = (String) maps.stream().filter(i -> i.get("NAME").toString().contains("pool-1-thread") &&
                    i.get("STATE").toString().equals("TIMED_WAITING")).map(i -> i.get("ID")).findFirst().get();
            Assert.assertFalse(maps.isEmpty());
            execute(mycatConnection, KillThreadHint.create(Long.parseLong(id)));

            maps = executeQuery(mycatConnection, ShowThreadInfoHint.create());
            //Assert.assertTrue(maps.stream().noneMatch(i -> i.get("ID").toString().equals(id)));
            System.out.println();
        }
    }

    @Ignore
    public void testInterrupt() throws Exception {
        List<Map<String, Object>> maps;
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);) {
            maps = executeQuery(mycatConnection, ShowThreadInfoHint.create());
            String id = (String) maps.stream().filter(i -> i.get("NAME").toString().contains("pool-1-thread") &&
                    i.get("STATE").toString().equals("TIMED_WAITING")).map(i -> i.get("ID")).findFirst().get();
            Assert.assertFalse(maps.isEmpty());
            execute(mycatConnection, InterruptThreadHint.create(Long.parseLong(id)));

            maps = executeQuery(mycatConnection, ShowThreadInfoHint.create());
            System.out.println();
        }
    }
}
