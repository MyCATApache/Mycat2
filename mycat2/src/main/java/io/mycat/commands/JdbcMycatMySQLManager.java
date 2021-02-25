//package io.mycat.commands;
//
//import io.mycat.MetaClusterCurrent;
//import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
//import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
//import io.mycat.vertxmycat.JdbcMySqlConnection;
//import io.vertx.core.Future;
//import io.vertx.sqlclient.SqlConnection;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//
//import static io.vertx.core.Future.succeededFuture;
//
//public class JdbcMycatMySQLManager extends AbstractMySQLManagerImpl {
//    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMycatMySQLManager.class);
//
//
//    public JdbcMycatMySQLManager() {
//
//    }
//
//    //
//    @Override
//    public Future<SqlConnection> getConnection(String targetName) {
//        return Future.succeededFuture(new JdbcMySqlConnection(targetName));
//    }
//
//    @Override
//    public Future<Map<String, SqlConnection>> getConnectionMap() {
//        JdbcConnectionManager connectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
//        return getMapFuture(new HashSet<>(connectionManager.getDatasourceInfo().keySet()));
//    }
//
//    @Override
//    public Future<Void> close() {
//        return succeededFuture();
//    }
//
//    @Override
//    public Future<> Map<String, Integer> computeConnectionUsageSnapshot() {
//
//        HashMap<String, Integer> map = new HashMap<>();
//        JdbcConnectionManager connectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
//        Map<String, JdbcDataSource> datasourceInfo = connectionManager.getDatasourceInfo();
//        datasourceInfo.forEach((k, v) -> {
//            map.put(k, v.getMaxCon() - v.getUsedCount());
//        });
//        return map;
//    }
//
//}
