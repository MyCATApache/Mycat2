package io.mycat.assemble;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.hint.BaselineAddHint;
import io.mycat.hint.BaselineListHint;
import io.mycat.hint.BaselineUpdateHint;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class SpmHintTest implements MycatTest {

    @Test
    public void testAdd() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
        ) {
            List<Map<String, Object>> maps = executeQuery(mycatConnection, BaselineAddHint.create("/*+ mycat:xxx*/ select 1"));
            Assert.assertEquals(1, maps.size());
            Map<String, Object> map = maps.get(0);
            Assert.assertNotNull(map.get("BASELINE_ID"));
            Assert.assertEquals("OK", (map.get("STATUS")));
        }
    }

    @Test
    public void testFix() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
        ) {
            List<Map<String, Object>> maps = executeQuery(mycatConnection, BaselineAddHint.create(true, "/*+ mycat:xxx*/ select 1"));
            Assert.assertEquals(1, maps.size());
            Map<String, Object> map = maps.get(0);
            Assert.assertNotNull((map.get("BASELINE_ID")));
            Assert.assertEquals("OK", (map.get("STATUS")));
        }
    }

    @Test
    public void testList() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
        ) {
            List<Map<String, Object>> maps = executeQuery(mycatConnection, BaselineListHint.create());
            Assert.assertTrue(!maps.isEmpty());
            Map<String, Object> map = maps.get(0);
            Assert.assertNotNull(map.get("BASELINE_ID"));
            Assert.assertNotNull(map.get("PARAMETERIZED_SQL"));
            Assert.assertNotNull(map.get("FIXED"));
            Assert.assertNotNull(map.get("ACCEPTED"));
        }
    }

    @Test
    public void testPersistBaseline() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection prototypeDs = getMySQLConnection(DB1);) {
            deleteData(prototypeDs, "mycat", "spm_baseline");
            List<Map<String, Object>> maps = executeQuery(mycatConnection, BaselineListHint.create());
            Assert.assertTrue(!maps.isEmpty());
            Map<String, Object> map = maps.get(0);
            long baseline_id = Long.parseLong(Objects.toString(map.get("BASELINE_ID")));
            long plan_id = Long.parseLong(Objects.toString(map.get("PLAN_ID")));

            //PERSIST_PLAN
            deleteData(prototypeDs, "mycat", "spm_plan");
            JdbcUtils.execute(mycatConnection, BaselineUpdateHint.create("PERSIST_PLAN", plan_id));
            Assert.assertTrue(hasData(prototypeDs, "mycat", "spm_plan"));

            //CLEAR_PLAN
            JdbcUtils.execute(mycatConnection, BaselineUpdateHint.create("CLEAR_PLAN", plan_id));
            Assert.assertEquals(maps.size() - 1, executeQuery(mycatConnection, BaselineListHint.create()).size());

            //LOAD_PLAN
            JdbcUtils.execute(mycatConnection, BaselineUpdateHint.create("LOAD_PLAN", plan_id));
            Assert.assertEquals(maps.size(), executeQuery(mycatConnection, BaselineListHint.create()).size());

            //DELETE_PLAN
            long planCount = count(prototypeDs, "mycat", "spm_plan");
            JdbcUtils.execute(mycatConnection, BaselineUpdateHint.create("DELETE_PLAN", plan_id));
            Assert.assertTrue(planCount - 1 == count(prototypeDs, "mycat", "spm_plan"));

            //PERSIST
            JdbcUtils.execute(mycatConnection, BaselineUpdateHint.create("PERSIST", baseline_id));
            Assert.assertTrue(hasData(prototypeDs, "mycat", "spm_baseline"));
            Assert.assertTrue(hasData(prototypeDs, "mycat", "spm_plan"));

            //CLEAR
            maps = executeQuery(mycatConnection, BaselineListHint.create());
            map = maps.get(0);
            baseline_id = Long.parseLong(Objects.toString(map.get("BASELINE_ID")));
            JdbcUtils.execute(mycatConnection, BaselineUpdateHint.create("CLEAR", baseline_id));

            //DELETE
            long baselineCount = count(prototypeDs, "mycat", "spm_baseline");
            JdbcUtils.execute(mycatConnection, BaselineUpdateHint.create("DELETE", baseline_id));
            Assert.assertEquals(baselineCount - 1, count(prototypeDs, "mycat", "spm_baseline"));

            executeQuery(mycatConnection, "select 1.2");
            maps = executeQuery(mycatConnection, BaselineListHint.create());
            baseline_id = Long.parseLong(Objects.toString(maps.get(maps.size() - 1).get("BASELINE_ID")));
            JdbcUtils.execute(mycatConnection, BaselineUpdateHint.create("PERSIST", baseline_id));
            JdbcUtils.execute(mycatConnection, BaselineUpdateHint.create("CLEAR", baseline_id));

            //LOAD
            JdbcUtils.execute(mycatConnection, BaselineUpdateHint.create("LOAD", baseline_id));
            Assert.assertEquals(maps.size(), executeQuery(mycatConnection, BaselineListHint.create()).size());

            //DELETE_PLAN
            executeQuery(mycatConnection, "select 1.2");
            maps = executeQuery(mycatConnection, BaselineListHint.create());
            map = maps.get(0);
            plan_id = Long.parseLong(Objects.toString(map.get("PLAN_ID")));
            long planCount0 = count(prototypeDs, "mycat", "spm_plan");
            JdbcUtils.execute(mycatConnection, BaselineUpdateHint.create("PERSIST_PLAN", plan_id));
            planCount = count(prototypeDs, "mycat", "spm_plan");
            baselineCount = count(prototypeDs, "mycat", "spm_baseline");
            JdbcUtils.execute(mycatConnection, BaselineUpdateHint.create("DELETE_PLAN", plan_id));
            Assert.assertEquals(planCount - 1, count(prototypeDs, "mycat", "spm_plan"));
            Assert.assertEquals(baselineCount, count(prototypeDs, "mycat", "spm_baseline"));


        }
    }
}
