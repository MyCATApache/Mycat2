/**
 * Copyright (C) <2021>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.runtime;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaLog;
import com.alibaba.druid.sql.SQLUtils;
import io.mycat.*;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.util.packet.AbstractWritePacket;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Getter
@Setter
public class MycatDataContextImpl implements MycatDataContext {
    final static Logger log = LoggerFactory.getLogger(MycatDataContextImpl.class);
    private final long id;
    private String defaultSchema;
    private String lastMessage;
    private long affectedRows;
    private int warningCount;
    private long lastInsertId;
    private int serverCapabilities;
    private int lastErrorCode;
    private static final String state = "HY000";
    private String sqlState = state;

    private String charsetName;
    private Charset charset;
    private int charsetIndex;

    protected int localInFileRequestState = 0;
    private long selectLimit = -1;
    private long netWriteTimeout = -1;
    private boolean readOnly = false;

    public int multiStatementSupport = 0;
    private String charsetSetResult;
    private volatile boolean inTransaction = false;

    private MycatUser user;
    private TransactionSession transactionSession;
    private final AtomicBoolean cancelFlag = new AtomicBoolean(false);
    private final Map<Long, PreparedStatement> preparedStatementMap = new HashMap<>();

    public static final AtomicLong IDS = new AtomicLong();

    private final AtomicLong prepareStatementIds = new AtomicLong(0);
    private ObservableEmitter<AbstractWritePacket> emitter;
    private volatile Observable<AbstractWritePacket> observable;
    private Map<String, Object> processStateMap;

    public MycatDataContextImpl() {
        this.id = IDS.getAndIncrement();
        switchTransaction(TransactionType.DEFAULT);
    }


    @Override
    public long getSessionId() {
        return id;
    }

    @Override
    public TransactionType transactionType() {
        return transactionSession.transactionType();
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
        Objects.requireNonNull(transactionSessionType);
        TransactionSession transactionSession = null;
        switch (transactionSessionType) {
            case PROXY_TRANSACTION_TYPE:
                transactionSession = new ProxyTransactionSession(() -> MetaClusterCurrent.wrapper(MySQLManager.class), MetaClusterCurrent.wrapper(XaLog.class));
                break;
            case JDBC_TRANSACTION_TYPE:
                transactionSession = new XaTransactionSession(() -> MetaClusterCurrent.wrapper(MySQLManager.class), MetaClusterCurrent.wrapper(XaLog.class));
                break;
            default:
                throw new IllegalStateException("Unexpected transaction type: " + transactionSessionType);
        }
        if (this.transactionSession != null) {
            this.transactionSession.deliverTo(transactionSession);

        }
        this.transactionSession = transactionSession;
    }

    public void setUser(MycatUser user) {
        this.user = user;
        String transactionType = user.getUserConfig().getTransactionType();
        switchTransaction(TransactionType.parse(transactionType));
    }

    @Override
    public Object getVariable(boolean global, String target) {
        if (global) {
            MysqlVariableService variableService = MetaClusterCurrent.wrapper(MysqlVariableService.class);
            return variableService.getGlobalVariable(target);
        }
        if (target.contains("autocommit")) {
            return this.isAutocommit() ? "1" : "0";
        } else if (target.equalsIgnoreCase("xa")) {
            return this.transactionType() == TransactionType.JDBC_TRANSACTION_TYPE ? "1" : "0";
        } else if (target.contains("net_write_timeout")) {
            return this.getVariable(MycatDataContextEnum.NET_WRITE_TIMEOUT);
        } else if ("sql_select_limit".equalsIgnoreCase(target)) {
            return this.getVariable(MycatDataContextEnum.SELECT_LIMIT);
        } else if ("character_set_results".equalsIgnoreCase(target)) {
            return this.getVariable(MycatDataContextEnum.CHARSET_SET_RESULT);
        } else if (target.contains("read_only")) {
            return this.getVariable(MycatDataContextEnum.IS_READ_ONLY);
        } else if (target.contains("current_user")) {
            return this.getUser().getUserName();
        } else if (target.contains("transaction_policy")) {
            return this.transactionType().getName();
        }
        MysqlVariableService variableService = MetaClusterCurrent.wrapper(MysqlVariableService.class);
        return variableService.getVariable(target);
    }

    @Override
    public void setVariable(MycatDataContextEnum name, Object value) {
        switch (name) {
            case DEFAULT_SCHEMA:
                setDefaultSchema((String) value);
                break;
            case IS_MULTI_STATEMENT_SUPPORT:
                setMultiStatementSupport(((Number) value).intValue());
                break;
            case IS_LOCAL_IN_FILE_REQUEST_STATE:
                setLocalInFileRequestState(((Number) value).intValue());
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
                return multiStatementSupport;
            case IS_LOCAL_IN_FILE_REQUEST_STATE:
                return localInFileRequestState;
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
                return isReadOnly()?1:0;
            }
            case IS_IN_TRANSCATION: {
                return isInTransaction() ? 1 : 0;
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

    @Override
    public boolean isAutocommit() {
        return transactionSession.isAutocommit();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        transactionSession.setAutocommit(autoCommit);
    }

    @Override
    public MySQLIsolation getIsolation() {
        return transactionSession.getTransactionIsolation();
    }

    @Override
    public void setIsolation(MySQLIsolation isolation) {
        this.transactionSession.setTransactionIsolation(isolation);
    }

    @Override
    public boolean isInTransaction() {
        return transactionSession.isInTransaction();
    }

    @Override
    public void setInTransaction(boolean inTransaction) {
        this.inTransaction = inTransaction;
    }

    @Override
    public MycatUser getUser() {
        return user;
    }

    @Override
    public void useShcema(String schema) {
        this.setDefaultSchema(SQLUtils.normalize(schema));
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
    @Override
    public String resolveDatasourceTargetName(String targetName) {
        return resolveDatasourceTargetName(targetName, false);
    }

    @Override
    public String resolveDatasourceTargetName(String targetName, boolean master) {
        ReplicaBalanceType balanceType = ReplicaBalanceType.NONE;
        if (processStateMap != null) {
            balanceType = (ReplicaBalanceType) processStateMap.getOrDefault("REP_BALANCE_TYPE", ReplicaBalanceType.NONE);
        }
        return transactionSession.resolveFinalTargetName(targetName, master, balanceType);
    }

    @Override
    public Map<Long, PreparedStatement> getPrepareInfo() {
        return preparedStatementMap;
    }

    @Override
    public long nextPrepareStatementId() {
        return prepareStatementIds.getAndIncrement();
    }

    @Override
    public void close() {
        if (transactionSession != null) {
            transactionSession.closeStatementState();
            transactionSession.close();
        }
        cancelFlag.set(true);
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        return (int) id;
    }

    @Override
    public void setLastInsertId(long lastInsertId) {
        this.lastInsertId = lastInsertId;
    }

    @Override
    public Map<String, Object> getProcessStateMap() {
        return processStateMap;
    }

    @Override
    public void putProcessStateMap(Map<String, Object> map) {
        this.processStateMap = map;
    }
}