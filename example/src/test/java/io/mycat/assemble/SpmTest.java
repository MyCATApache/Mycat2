package io.mycat.assemble;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.*;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.calcite.spm.Baseline;
import io.mycat.calcite.spm.DbPlanManagerPersistorImpl;
import io.mycat.calcite.spm.MemPlanCache;
import io.mycat.calcite.spm.PlanResultSet;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.config.ClusterConfig;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.ServerConfig;
import io.mycat.datasource.jdbc.DruidDatasourceProvider;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.replica.ReplicaSelectorManager;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.util.NameMap;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import org.apache.groovy.util.Maps;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class SpmTest implements MycatTest {
    final DbPlanManagerPersistorImpl dbPlanManagerPersistor = new DbPlanManagerPersistorImpl();

    static JdbcConnectionManager jdbcManager;

    @BeforeClass
    public static void init() throws Exception {
        HashMap<Class, Object> context = new HashMap<>();
        context.put(Vertx.class, Vertx.vertx());
        context.put(ServerConfig.class, new ServerConfig());
        context.put(IOExecutor.class,IOExecutor.DEFAULT);
        context.put(DrdsSqlCompiler.class, new DrdsSqlCompiler(new DrdsConst() {
            @Override
            public NameMap<SchemaHandler> schemas() {
                return new NameMap<>();
            }

            @Override
            public boolean joinClustering() {
                return true;
            }
        }));
        MetaClusterCurrent.register(context);
        String customerDatasourceProvider = DruidDatasourceProvider.class.getName();
        DatasourceConfig datasourceConfig = new DatasourceConfig();
        datasourceConfig.setDbType("mysql");
        datasourceConfig.setUser("root");
        datasourceConfig.setPassword("123456");
        datasourceConfig.setName("prototypeDs");
        datasourceConfig.setUrl("jdbc:mysql://localhost:3306/mysql");
        Map<String, DatasourceConfig> datasources = Maps.of("prototypeDs", datasourceConfig);

        ClusterConfig clusterConfig = new ClusterConfig();
        clusterConfig.setName("prototype");
        clusterConfig.setMasters(Arrays.asList("prototypeDs"));

        Map<String, ClusterConfig> clusterConfigs = Maps.of("prototype", clusterConfig);

        LinkedList<Runnable> runnables = new LinkedList<>();
        ReplicaSelectorManager manager = ReplicaSelectorRuntime.create(
                new ArrayList<>(clusterConfigs.values()),
                datasources,
                new LoadBalanceManager(),
                name -> 0,
                (command, initialDelay, period, unit) -> {
                    runnables.add(command);
                    return () -> {

                    };
                }
        );
        context.put(JdbcConnectionManager.class, jdbcManager = new JdbcConnectionManager(DruidDatasourceProvider.class.getName(),
                datasources,
                clusterConfigs,
                manager
        ));
        MetaClusterCurrent.register(context);
    }

    @AfterClass
    public static void close() throws Exception {
        try (DefaultConnection defaultConnection = jdbcManager.getConnection("prototypeDs");) {
            Connection rawConnection = defaultConnection.getRawConnection();
            Statement statement = rawConnection.createStatement();
            statement.execute("delete from mycat.spm_baseline");
            statement.execute("delete from mycat.spm_plan");
        }
        MetaClusterCurrent.wrapper(JdbcConnectionManager.class).close();
    }

    @Test
    @SneakyThrows
    public void testCheckStore() {
        try (DefaultConnection defaultConnection = jdbcManager.getConnection("prototypeDs");) {
            Connection rawConnection = defaultConnection.getRawConnection();
            JdbcUtils.execute(rawConnection, "DROP TABLE IF EXISTS mycat.spm_baseline");
            JdbcUtils.execute(rawConnection, "DROP TABLE IF EXISTS mycat.spm_plan");
            dbPlanManagerPersistor.checkStore();
            JdbcUtils.executeQuery(rawConnection, "SELECT * FROM mycat.spm_baseline", Collections.emptyList());
            JdbcUtils.executeQuery(rawConnection, "SELECT * FROM mycat.spm_plan", Collections.emptyList());
        }
    }


    @Test
    @SneakyThrows
    public void testAdd() {
        boolean fix = false;
        testAdd(fix);
    }
    @Test
    @SneakyThrows
    public void testFix() {
        boolean fix = true;
        testAdd(fix);
    }
    private void testAdd(boolean fix) throws Exception {
        dbPlanManagerPersistor.checkStore();
        try (DefaultConnection defaultConnection = jdbcManager.getConnection("prototypeDs");) {
            Connection rawConnection = defaultConnection.getRawConnection();
            deleteData(rawConnection, "mycat", "spm_baseline");
            deleteData(rawConnection, "mycat", "spm_plan");
            MemPlanCache memPlanCache = new MemPlanCache(dbPlanManagerPersistor);

            DrdsSqlWithParams n = DrdsRunnerHelper.preParse("select 1", null);
            DrdsSqlWithParams c = DrdsRunnerHelper.preParse("select '1'", null);

            memPlanCache.getBaseline(n);
            memPlanCache.getBaseline(c);

            Assert.assertFalse(hasData(rawConnection, "mycat", "spm_baseline"));
            Assert.assertFalse(hasData(rawConnection, "mycat", "spm_plan"));

            //saveBaselines
            memPlanCache.saveBaselines();

            Assert.assertTrue(hasData(rawConnection, "mycat", "spm_baseline"));
            Assert.assertFalse(hasData(rawConnection, "mycat", "spm_plan"));

            PlanResultSet add1 = memPlanCache.add(fix, n);
            Assert.assertTrue(add1.isOk());
            if (fix){
              Assert.assertTrue(null!=  memPlanCache.getBaseline(n).getFixPlan());
            }
            PlanResultSet add2 = memPlanCache.add(fix, c);
            if (fix){
                Assert.assertTrue(null!=  memPlanCache.getBaseline(n).getFixPlan());
            }
            Assert.assertTrue(add2.isOk());

            //saveBaselines
            memPlanCache.saveBaselines();

            Assert.assertTrue(hasData(rawConnection, "mycat", "spm_plan"));

            //list
            List<Baseline> list = memPlanCache.list();
            Assert.assertEquals(2, list.size());

            Assert.assertTrue(list.contains(memPlanCache.getBaseline(n)));
            Assert.assertTrue(list.contains(memPlanCache.getBaseline(c)));


            long planId = memPlanCache.getBaseline(c).getPlanList().stream().findFirst().get().getId();


            Baseline backupBaseline = memPlanCache.getBaseline(n);


            Baseline baseline = memPlanCache.getBaseline(n);

            //persistPlan
            memPlanCache.persistPlan(planId);
            Assert.assertFalse(JdbcUtils.executeQuery(rawConnection, "select * from mycat.spm_plan where id = ?", Arrays.asList(planId)).isEmpty());


            //loadPlan
            memPlanCache.loadPlan(planId);
            Assert.assertFalse(memPlanCache.getBaseline(c).getPlanList().isEmpty());
            Assert.assertFalse(baseline.getPlanList().isEmpty());

            //deletePlan
            memPlanCache.deletePlan(planId);
            Assert.assertTrue(JdbcUtils.executeQuery(rawConnection, "select * from mycat.spm_plan where id = ?", Arrays.asList(planId)).isEmpty());

            //persistPlan
            memPlanCache.persistPlan(planId);
            Assert.assertFalse(JdbcUtils.executeQuery(rawConnection, "select * from mycat.spm_plan where id = ?", Arrays.asList(planId)).isEmpty());

            //deleteBaseline
            memPlanCache.deleteBaseline(baseline.getBaselineId());
            Assert.assertTrue(JdbcUtils.executeQuery(rawConnection, "select * from mycat.spm_baseline where id = ?", Arrays.asList(baseline.getBaselineId())).isEmpty());

            //persistBaseline
            memPlanCache.persistBaseline(baseline.getBaselineId());
            Assert.assertTrue(!JdbcUtils.executeQuery(rawConnection, "select * from mycat.spm_baseline where id = ?", Arrays.asList(baseline.getBaselineId())).isEmpty());

            //deleteBaseline
            memPlanCache.deleteBaseline(baseline.getBaselineId());
            Assert.assertTrue(JdbcUtils.executeQuery(rawConnection, "select * from mycat.spm_baseline where id = ?", Arrays.asList(baseline.getBaselineId())).isEmpty());

            if (fix){
                Assert.assertTrue(null!=     memPlanCache.getBaseline(baseline.getBaselineId()).getFixPlan());
                Assert.assertTrue(null!=     memPlanCache.getBaseline(n).getFixPlan());
                Assert.assertTrue(null!=     memPlanCache.getBaseline(c).getFixPlan());
            }
            //persistBaseline
            memPlanCache.persistBaseline(baseline.getBaselineId());
            Assert.assertTrue(!JdbcUtils.executeQuery(rawConnection, "select * from mycat.spm_baseline where id = ?", Arrays.asList(baseline.getBaselineId())).isEmpty());

            //loadBaseline
            memPlanCache.loadBaseline(baseline.getBaselineId());
            Assert.assertFalse(baseline.getPlanList().isEmpty());

            if (fix){
                Assert.assertTrue(null!=     memPlanCache.getBaseline(baseline.getBaselineId()).getFixPlan());
                Assert.assertTrue(null!=     memPlanCache.getBaseline(n).getFixPlan());
                Assert.assertTrue(null!=     memPlanCache.getBaseline(c).getFixPlan());
            }

            //clearPlan
            memPlanCache.clearPlan(planId);
            Assert.assertTrue(memPlanCache.getBaseline(c).getPlanList().isEmpty());

            //clearBaseline
            memPlanCache.clearBaseline(memPlanCache.getBaseline(n).getBaselineId());
            Assert.assertNotEquals(backupBaseline.getBaselineId(), memPlanCache.getBaseline(n).getBaselineId());
            memPlanCache.clearBaseline(memPlanCache.getBaseline(n).getBaselineId());
        }
    }
}
