//package io.mycat.util;
//
//import io.mycat.ExplainDetail;
//import io.mycat.api.collector.RowBaseIterator;
//import io.mycat.api.collector.RowIterable;
//import io.mycat.datasource.jdbc.JdbcRuntime;
//import io.mycat.datasource.jdbc.datasource.DefaultConnection;
//import io.mycat.replica.ReplicaSelectorRuntime;
//
//public class CacheResponse implements Response {
//
//    @Override
//    public void sendError(Throwable e) {
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public void proxySelect(String defaultTargetName, String statement) {
//        String replicaName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(defaultTargetName, false, null);
//        DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(defaultTargetName);
//        RowBaseIterator rowBaseIterator = connection.executeQuery(statement);
//    }
//
//    @Override
//    public void proxyUpdate(String defaultTargetName, String proxyUpdate) {
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public void tryBroadcastShow(String statement) {
//
//    }
//
//    @Override
//    public void sendError(String errorMessage, int errorCode) {
//
//    }
//
//    @Override
//    public void sendResultSet(RowIterable rowIterable) {
//
//    }
//
//    @Override
//    public void rollback() {
//
//    }
//
//    @Override
//    public void begin() {
//
//    }
//
//    @Override
//    public void commit() {
//
//    }
//
//    @Override
//    public void execute(ExplainDetail detail) {
//
//    }
//
//    @Override
//    public void sendOk(long lastInsertId, long affectedRow) {
//
//    }
//}