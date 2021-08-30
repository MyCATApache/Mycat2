//package io.mycat.commands;
//
//import io.mycat.monitor.DatabaseInstanceEntry;
//import io.mycat.newquery.NewMycatConnection;
//import io.vertx.core.Future;
//import io.vertx.sqlclient.SqlConnection;
//
//public class MonitorMycatDatasourcePool implements MycatDatasourcePool {
//    final MycatDatasourcePool pool;
//    public MonitorMycatDatasourcePool(MycatDatasourcePool pool) {
//        this.pool = pool;
//    }
//
//    @Override
//    public Future<NewMycatConnection> getConnection() {
//        return pool.getConnection().map(sqlConnection -> {
//            DatabaseInstanceEntry stat = DatabaseInstanceEntry.stat(pool.getTargetName());
//            stat.plusCon();
//            return new MonitorSqlConnection(sqlConnection, stat, MonitorMycatDatasourcePool.this);
//        });
//        return null;
//    }
//
//    @Override
//    public Integer getAvailableNumber() {
//        return pool.getAvailableNumber();
//    }
//
//    @Override
//    public Integer getUsedNumber() {
//        return pool.getUsedNumber();
//    }
//
//    @Override
//    public String getTargetName() {
//        return pool.getTargetName();
//    }
//}
