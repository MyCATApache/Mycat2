//package io.mycat.mock;
//
//import io.mycat.MycatConnection;
//import io.mycat.upondb.MycatDBClientBasedConfig;
//import io.mycat.upondb.MycatDBClientMediator;
//import io.mycat.upondb.MycatDBSharedServer;
//import io.mycat.util.SQLContext;
//import lombok.AllArgsConstructor;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Map;
//import java.util.concurrent.atomic.AtomicBoolean;
//
//@AllArgsConstructor
//public class MockDBClientMediator extends MycatDBClientMediator {
//    final static Logger log = LoggerFactory.getLogger(MockDBClientMediator.class);
//    final MockDBSharedServer sharedServer = new MockDBSharedServer();
//    final MycatDBClientBasedConfig clientBasedConfig;
//    @Override
//    public MycatDBSharedServer getUponDBSharedServer() {
//        return sharedServer;
//    }
//
//    @Override
//    public MycatDBClientBasedConfig config() {
//        return clientBasedConfig;
//    }
//
//    @Override
//    public Map<String, Object> variables() {
//        return null;
//    }
//
//    @Override
//    public MycatConnection getConnection(String targetName) {
//        return null;
//    }
//
//
//    @Override
//    public String getSchema() {
//        return null;
//    }
//
//    @Override
//    public void begin() {
//
//    }
//
//    @Override
//    public void rollback() {
//
//    }
//
//    @Override
//    public void useSchema(String normalize) {
//
//    }
//
//    @Override
//    public void commit() {
//
//    }
//
//    @Override
//    public void setTransactionIsolation(int value) {
//
//    }
//
//    @Override
//    public int getTransactionIsolation() {
//        return 0;
//    }
//
//    @Override
//    public boolean isAutocommit() {
//        return false;
//    }
//
//    @Override
//    public void setAutocommit(boolean autocommit) {
//
//    }
//
//    @Override
//    public boolean isAutoCommit() {
//        return false;
//    }
//
//    @Override
//    public boolean isInTransaction() {
//        return false;
//    }
//
//    @Override
//    public long getMaxRow() {
//        return 0;
//    }
//
//    @Override
//    public void setMaxRow(long value) {
//
//    }
//
//    @Override
//    public void setAutoCommit(boolean autocommit) {
//
//    }
//
//    @Override
//    public void close() {
//
//    }
//
//    @Override
//    public AtomicBoolean cancelFlag() {
//        return null;
//    }
//
//    @Override
//    public String resolveFinalTargetName(String targetName) {
//        return targetName;
//    }
//
//    @Override
//    public void addCloseResource(AutoCloseable connection) {
//
//    }
//
//    @Override
//    public void recycleResource() {
//
//    }
//
//    @Override
//    public int getServerStatus() {
//        return 0;
//    }
//
//    @Override
//    public SQLContext sqlContext() {
//        return null;
//    }
//
//    @Override
//    public long lastInsertId() {
//        return 0;
//    }
//
//    @Override
//    public void setVariable(String target, Object value) {
//
//    }
//
//    @Override
//    public Object getVariable(String target) {
//        return null;
//    }
//}