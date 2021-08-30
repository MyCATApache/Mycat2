//package io.mycat.commands;
//
//import io.mycat.monitor.DatabaseInstanceEntry;
//import io.mycat.newquery.MysqlCollector;
//import io.mycat.newquery.NewMycatConnection;
//import io.mycat.newquery.RowSet;
//import io.mycat.newquery.SqlResult;
//import io.vertx.core.AsyncResult;
//import io.vertx.core.Future;
//import io.vertx.core.Handler;
//import io.vertx.sqlclient.*;
//import io.vertx.sqlclient.spi.DatabaseMetadata;
//
//import java.util.List;
//
//public class MonitorSqlConnection implements NewMycatConnection {
//    final NewMycatConnection sqlConnection;
//    private DatabaseInstanceEntry stat;
//    final private NewMycatConnection monitorMycatDatasourcePool2;
//
//    public MonitorSqlConnection(NewMycatConnection sqlConnection, DatabaseInstanceEntry stat, MonitorMycatDatasourcePool monitorMycatDatasourcePool2) {
//        this.sqlConnection = sqlConnection;
//        this.stat = stat;
//        this.monitorMycatDatasourcePool2 = monitorMycatDatasourcePool2;
//    }
//
//    @Override
//    public Future<RowSet> query(String sql, List<Object> params) {
//        return sqlConnection.query(sql,params);
//    }
//
//    @Override
//    public void prepareQuery(String sql, List<Object> params, MysqlCollector collector) {
//        sqlConnection.prepareQuery(sql,params,collector);
//    }
//
//    @Override
//    public Future<SqlResult> insert(String sql, List<Object> params) {
//        return null;
//    }
//
//    @Override
//    public Future<SqlResult> insert(String sql) {
//        return null;
//    }
//
//    @Override
//    public Future<SqlResult> update(String sql) {
//        return null;
//    }
//
//    @Override
//    public Future<SqlResult> update(String sql, List<Object> params) {
//        return null;
//    }
//
//    @Override
//    public Future<Void> close() {
//        return null;
//    }
//}
