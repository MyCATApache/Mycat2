package io.mycat.drdsrunner;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.*;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.config.ClusterConfig;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.ServerConfig;
import io.mycat.datasource.jdbc.DruidDatasourceProvider;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.drdsrunner.DrdsTest;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.replica.ReplicaSelectorManager;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.statistic.StatisticCenter;
import io.mycat.util.NameMap;
import io.vertx.core.Vertx;
import org.apache.groovy.util.Maps;
import org.junit.*;
import org.junit.jupiter.api.Disabled;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
@Disabled
@Ignore
public class StatisticCenterTest extends DrdsTest {


    static JdbcConnectionManager jdbcManager;
    static StatisticCenter statisticCenter = new StatisticCenter();

    @BeforeClass
    public static void init() throws Exception {
        HashMap<Class, Object> context = new HashMap<>();
        context.put(Vertx.class, Vertx.vertx());
        context.put(ServerConfig.class, new ServerConfig());
        context.put(DrdsSqlCompiler.class, new DrdsSqlCompiler(new DrdsConst() {
            @Override
            public NameMap<SchemaHandler> schemas() {
                return new NameMap<>();
            }

            @Override
            public boolean bkaJoin() {
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
        context.put(ReplicaSelectorManager.class,manager);
        context.put(JdbcConnectionManager.class, jdbcManager = new JdbcConnectionManager(DruidDatasourceProvider.class.getName(),
                datasources,
                clusterConfigs,
                manager
        ));
        MetaClusterCurrent.register(context);
        statisticCenter.init();
    }

    @AfterClass
    public static void close() throws Exception {
        try (DefaultConnection defaultConnection = jdbcManager.getConnection("prototypeDs");) {
            Connection rawConnection = defaultConnection.getRawConnection();
            Statement statement = rawConnection.createStatement();
            statement.execute("delete from mycat.analyze_table");
        }
        MetaClusterCurrent.wrapper(JdbcConnectionManager.class).close();
    }


    @Test
    @Disabled
    @Ignore
    public void testNormal() throws Exception{
        DrdsSqlCompiler drds = getDrds();
        String schemaName = "db1";
        String tableName = "normal";
        MetadataManager metadataManager = getMetadataManager();
        TableHandler tableHandler = metadataManager.getTable(schemaName, tableName);
        if (tableHandler == null) {
            return;
        }
        tableHandler.createPhysicalTables();
        NormalTable table = (NormalTable) tableHandler;
        try (DefaultConnection connection = jdbcManager.getConnection("prototype")) {
            deleteData(connection.getRawConnection(), "mycat", "analyze_table");
        }
        try (DefaultConnection connection = jdbcManager.getConnection(table.getDataNode().getTargetName())) {
            deleteData(connection.getRawConnection(), schemaName, tableName);
            JdbcUtils.execute(connection.getRawConnection(), "insert db1.normal(id,addressname) values(?,?)", Arrays.asList(1, "a"));
        }
        Double count = statisticCenter.computeTableRowCount(tableHandler);
        Assert.assertTrue(count.equals(count));

        statisticCenter.fetchTableRowCount(tableHandler);

        try (DefaultConnection connection = jdbcManager.getConnection("prototype")) {
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(connection.getRawConnection(), "select * from mycat.analyze_table ", Arrays.asList());
            Assert.assertTrue(maps.toString().contains(tableName));
        }
    }

    @Test
    @Disabled
    @Ignore
    public void testGlobal() throws Exception{
        DrdsSqlCompiler drds = getDrds();
        String schemaName = "db1";
        String tableName = "global";
        MetadataManager metadataManager = getMetadataManager();
        TableHandler tableHandler = metadataManager.getTable(schemaName, tableName);
        if (tableHandler == null) {
            return;
        }
        tableHandler.createPhysicalTables();
        GlobalTable table = (GlobalTable) tableHandler;
        try (DefaultConnection connection = jdbcManager.getConnection("prototype")) {
            deleteData(connection.getRawConnection(), schemaName, tableName);
            JdbcUtils.execute(connection.getRawConnection(), "insert db1.global(id) values(?)", Arrays.asList(1));
        }
        Double count = statisticCenter.computeTableRowCount(tableHandler);
        Assert.assertTrue(count.equals(count));

        statisticCenter.fetchTableRowCount(tableHandler);

        try (DefaultConnection connection = jdbcManager.getConnection("prototype")) {
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(connection.getRawConnection(), "select * from mycat.analyze_table ", Arrays.asList());
            Assert.assertTrue(maps.toString().contains(tableName));
        }
    }

    @Test
    @Disabled
    @Ignore
    public void testSharding() throws Exception{
        DrdsSqlCompiler drds = getDrds();
        String schemaName = "db1";
        String tableName = "sharding";
        MetadataManager metadataManager = getMetadataManager();
        TableHandler tableHandler = metadataManager.getTable(schemaName, tableName);
        if (tableHandler == null) {
            return;
        }
        tableHandler.createPhysicalTables();
        ShardingTable table = (ShardingTable) tableHandler;
        List<Partition> backends = table.getBackends();
        Partition c0 = backends.get(0);
        Partition c1 = backends.get(1);
        try (DefaultConnection connection = jdbcManager.getConnection("prototype")) {
            deleteData(connection.getRawConnection(), schemaName, tableName);
            JdbcUtils.execute(connection.getRawConnection(), "insert db1.sharding(id) values(?)", Arrays.asList(1));
        }
        Double count = statisticCenter.computeTableRowCount(tableHandler);
        Assert.assertTrue(count.equals(count));

        statisticCenter.fetchTableRowCount(tableHandler);

        try (DefaultConnection connection = jdbcManager.getConnection("prototype")) {
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(connection.getRawConnection(), "select * from mycat.analyze_table ", Arrays.asList());
            Assert.assertTrue(maps.toString().contains(tableName));
        }
    }
}
