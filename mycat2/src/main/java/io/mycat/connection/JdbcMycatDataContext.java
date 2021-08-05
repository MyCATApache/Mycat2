package io.mycat.connection;

import io.mycat.*;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.sqlrecorder.SqlRecord;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class JdbcMycatDataContext implements MycatDataContext {
    @Override
    public long getSessionId() {
        return 0;
    }

    @Override
    public TransactionType transactionType() {
        return null;
    }

    @Override
    public TransactionSession getTransactionSession() {
        return null;
    }

    @Override
    public void setTransactionSession(TransactionSession session) {

    }

    @Override
    public void switchTransaction(TransactionType transactionSessionType) {

    }

    @Override
    public <T> T getVariable(boolean global, String target) {
        return null;
    }

    @Override
    public <T> T getVariable(MycatDataContextEnum name) {
        return null;
    }

    @Override
    public void setVariable(MycatDataContextEnum name, Object value) {

    }

    @Override
    public boolean isAutocommit() {
        return false;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {

    }

    @Override
    public MySQLIsolation getIsolation() {
        return null;
    }

    @Override
    public void setIsolation(MySQLIsolation isolation) {

    }

    @Override
    public boolean isInTransaction() {
        return false;
    }

    @Override
    public void setInTransaction(boolean inTransaction) {

    }

    @Override
    public MycatUser getUser() {
        return null;
    }

    @Override
    public void setUser(MycatUser user) {

    }

    @Override
    public void useShcema(String schema) {

    }

    @Override
    public String getDefaultSchema() {
        return null;
    }

    @Override
    public int getServerCapabilities() {
        return 0;
    }

    @Override
    public void setServerCapabilities(int serverCapabilities) {

    }

    @Override
    public int getWarningCount() {
        return 0;
    }

    @Override
    public long getLastInsertId() {
        return 0;
    }

    @Override
    public void setLastInsertId(long s) {

    }

    @Override
    public Charset getCharset() {
        return null;
    }

    @Override
    public int getCharsetIndex() {
        return 0;
    }

    @Override
    public int getLastErrorCode() {
        return 0;
    }

    @Override
    public long getAffectedRows() {
        return 0;
    }

    @Override
    public void setAffectedRows(long affectedRows) {

    }

    @Override
    public String getLastMessage() {
        return null;
    }

    @Override
    public void setLastMessage(String lastMessage) {

    }

    @Override
    public void setCharset(int index, String charsetName, Charset defaultCharset) {

    }

    @Override
    public AtomicBoolean getCancelFlag() {
        return null;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public String resolveDatasourceTargetName(String targetName) {
        return null;
    }

    @Override
    public String resolveDatasourceTargetName(String targetName, boolean master) {
        return null;
    }

    @Override
    public Map<Long, PreparedStatement> getPrepareInfo() {
        return null;
    }

    @Override
    public SqlRecord startSqlRecord() {
        return null;
    }

    @Override
    public SqlRecord currentSqlRecord() {
        return null;
    }

    @Override
    public void endSqlRecord() {

    }

    @Override
    public long nextPrepareStatementId() {
        return 0;
    }

    @Override
    public void setCharsetIndex(int characterSet) {

    }

    @Override
    public void setLastErrorCode(int errorCode) {

    }

    @Override
    public Map<String, Object> getProcessStateMap() {
        return null;
    }

    @Override
    public void putProcessStateMap(Map<String, Object> map) {

    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
