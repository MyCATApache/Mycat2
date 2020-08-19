package io.mycat.runtime;

import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.beans.mycat.JdbcRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.replica.ReplicaSelectorRuntime;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

@Getter
@Setter
public class MycatDataContextImpl implements MycatDataContext {
    final static Logger log = LoggerFactory.getLogger(MycatDataContextImpl.class);
    private TransactionType transactionType;
    private String defaultSchema;
    private String lastMessage;
    private long affectedRows;
    private int serverStatus;
    private int warningCount;
    private long lastInsertId;
    private int serverCapabilities;
    private int lastErrorCode;
    private static final String state = "HY000";
    private String sqlState = state;

    private String charsetName;
    private Charset charset;
    private int charsetIndex;


    private boolean autoCommit = true;
    private MySQLIsolation isolation = MySQLIsolation.REPEATED_READ;
    protected boolean localInFileRequestState = false;
    private long selectLimit = -1;
    private long netWriteTimeout = -1;
    private boolean readOnly = false;

    public boolean multiStatementSupport = false;
    private String charsetSetResult;
    private volatile boolean inTransaction = false;

    private MycatUser user;
    private TransactionSession transactionSession = new ProxyTransactionSession(this);
    private TransactionSessionRunner runner;
    private final AtomicBoolean cancelFlag = new AtomicBoolean(false);
    private final Map<Long,PreparedStatement> preparedStatementMap = new HashMap<>();
    public MycatDataContextImpl(TransactionSessionRunner runner) {
        this.runner = runner;
    }


    @Override
    public TransactionType transactionType() {
        return transactionType;
    }

    @Override
    public TransactionSession getTransactionSession() {
        return this.transactionSession;
    }

    @Override
    public void setTransactionSession(TransactionSession session) {
        this.transactionSession = session;
    }


    @Override
    public void switchTransaction(TransactionType transactionSessionType) {
        this.transactionType = transactionSessionType;
    }

    @Override
    public void setVariable(MycatDataContextEnum name, Object value) {
        switch (name) {
            case DEFAULT_SCHEMA:
                setDefaultSchema((String) value);
                break;
            case IS_MULTI_STATEMENT_SUPPORT:
                setMultiStatementSupport((Boolean) value);
                break;
            case IS_LOCAL_IN_FILE_REQUEST_STATE:
                setLocalInFileRequestState((Boolean) value);
                break;
            case AFFECTED_ROWS:
                setAffectedRows((Integer) value);
                break;
            case WARNING_COUNT:
                setWarningCount((Integer) value);
                break;
            case CHARSET_INDEX:
                setCharsetIndex((Integer) value);
                break;
            case IS_AUTOCOMMIT:
                setAutoCommit((Boolean) value);
                break;
            case ISOLATION:
                setIsolation((MySQLIsolation) value);
                break;
            case LAST_MESSAGE:
                setLastMessage((String) value);
                break;
            case LAST_INSERT_ID: {
                setLastInsertId(((Number) value).longValue());
                break;
            }
            case SERVER_CAPABILITIES:
                setServerCapabilities(((Number) value).intValue());
                break;
            case LAST_ERROR_CODE:
                setLastErrorCode(((Number) value).intValue());
                break;
            case SQL_STATE:
                setSqlState((String) value);
                break;
            case CHARSET_SET_RESULT:
                setCharsetSetResult((String) value);
                break;
            case SELECT_LIMIT:
                setSelectLimit(((Number) value).longValue());
                break;
            case NET_WRITE_TIMEOUT:
                setNetWriteTimeout(((Number) value).longValue());
                break;
            case IS_READ_ONLY: {
                setReadOnly((Boolean) value);
                break;
            }
            case IS_IN_TRANSCATION: {
                setInTransaction((Boolean) value);
                break;
            }
            case USER_INFO:
                user = (MycatUser) value;
                break;
        }
    }

    @Override
    public Object getVariable(MycatDataContextEnum name) {
        switch (name) {
            case DEFAULT_SCHEMA:
                return getDefaultSchema();
            case IS_MULTI_STATEMENT_SUPPORT:
                return isMultiStatementSupport();
            case IS_LOCAL_IN_FILE_REQUEST_STATE:
                return isLocalInFileRequestState();
            case AFFECTED_ROWS:
                return getAffectedRows();
            case WARNING_COUNT:
                return getWarningCount();
            case CHARSET_INDEX:
                return getCharsetIndex();
            case IS_AUTOCOMMIT:
                return isAutocommit();
            case ISOLATION:
                return getIsolation();
            case LAST_MESSAGE:
                return getLastMessage();
            case LAST_INSERT_ID: {
                return getLastInsertId();
            }
            case SERVER_CAPABILITIES:
                return getServerCapabilities();
            case LAST_ERROR_CODE:
                return getLastErrorCode();
            case SQL_STATE:
                return getSqlState();
            case CHARSET_SET_RESULT:
                return getCharsetSetResult();
            case SELECT_LIMIT:
                return getSelectLimit();
            case NET_WRITE_TIMEOUT:
                return getNetWriteTimeout();
            case IS_READ_ONLY: {
                return isReadOnly();
            }
            case IS_IN_TRANSCATION: {
                return isInTransaction();
            }
            case USER_INFO:
                return user;
        }
        throw new IllegalArgumentException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    public boolean isAutocommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public MySQLIsolation getIsolation() {
        return isolation;
    }

    public void setIsolation(MySQLIsolation isolation) {
        this.isolation = isolation;
    }

    public boolean isInTransaction() {
        return inTransaction;
    }

    public void setInTransaction(boolean inTransaction) {
        this.inTransaction = inTransaction;
    }

    public MycatUser getUser() {
        return user;
    }

    @Override
    public void useShcema(String schema) {
        this.setDefaultSchema(schema);
    }

    @Override
    public void setAffectedRows(long affectedRows) {
        this.affectedRows = affectedRows;
    }

    @Override
    public void setCharset(int index, String charsetName, Charset defaultCharset) {
        this.charsetIndex = index;
        this.charsetName = charsetName;
        this.charset = defaultCharset;
    }

    @Override
    public AtomicBoolean getCancelFlag() {
        return cancelFlag;
    }

    @Override
    public void run(Runnable runnable) {
        runner.run(this, runnable);
    }
//
//    @Override
//    public UpdateRowIteratorResponse update(String targetName, String sql) {
//        MycatConnection defaultConnection = TransactionSessionUtil.getDefaultConnection(targetName, true, null, transactionSession);
//        return defaultConnection.executeUpdate(sql, true, transactionSession.getServerStatus());
//    }
//    @Override
//    public RowBaseIterator query(String targetName, String sql){
//        MycatConnection defaultConnection = TransactionSessionUtil.getDefaultConnection(targetName, false, null, transactionSession);
//        return defaultConnection.executeQuery(null,sql);
//    }
//
//    @Override
//    public MycatRowMetaData queryMetaData(String targetName, String sql) {
//        String datasourceName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(targetName, false, null);
//        JdbcDataSource jdbcDataSource = JdbcRuntime.INSTANCE.getConnectionManager().getDatasourceInfo().get(datasourceName);
//        try(Connection connection1 = jdbcDataSource.getDataSource().getConnection()){
//            try(Statement statement = connection1.createStatement()){
//                try( ResultSet resultSet = statement.executeQuery(sql)) {
//                  return new JdbcRowMetaData(resultSet.getMetaData());
//                }
//            }
//        } catch (Throwable e) {
//            log.warn("",e);
//        }
//        return null;
//    }
//
//    @Override
//    public RowBaseIterator query(MycatRowMetaData mycatRowMetaData, String targetName, String sql) {
//        MycatConnection defaultConnection = TransactionSessionUtil.getDefaultConnection(targetName, false, null, transactionSession);
//        return defaultConnection.executeQuery(mycatRowMetaData,sql);
//    }
//
//    @Override
//    public RowBaseIterator queryDefaultTarget(String sql) {
//        String datasourceName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByRandom();
//        MycatConnection connection = transactionSession.getConnection(datasourceName);
//        Objects.requireNonNull(connection);
//        return connection.executeQuery(null,sql);
//    }

    public String resolveDatasourceTargetName(String targetName) {
        return transactionSession.resolveFinalTargetName(targetName);
    }

    @Override
    public Map<Long, PreparedStatement> getPrepareInfo() {
        return preparedStatementMap;
    }

    @Override
    public void close() {
        if (transactionSession != null) {
            transactionSession.check();
            transactionSession.close();
        }
        cancelFlag.set(true);
    }

    @Override
    public void block(Runnable runnable) {
        runner.block(this, runnable);
    }

}