package io.mycat;

import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLServerStatusFlags;
import io.mycat.sqlrecorder.SqlRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public interface MycatDataContext extends Wrapper, SessionOpt {
    static final Logger LOGGER = LoggerFactory.getLogger(MycatDataContext.class);

    long getSessionId();

    @Override
    TransactionSession getTransactionSession();

    void switchTransaction(TransactionType transactionSessionType);

    <T> T getVariable(boolean global, String target);

    <T> T getVariable(MycatDataContextEnum name);

    void setVariable(MycatDataContextEnum name, Object value);

    default void setVariable(String target,
                             Object text) {
        MySQLVariablesUtil.setVariable(this, target, text);
    }

    default int serverStatus() {
        MySQLServerStatusFlags.Builder builder = MySQLServerStatusFlags.builder();
        if (isAutocommit()) {
            builder.setAutoCommit();
        }
        if (isInTransaction()) {
            builder.setInTransaction();
        }
        if (isReadOnly()) {
            builder.setInTransReadonly();
        }
        return builder.build();
    }

    public boolean isAutocommit();

    public void setAutoCommit(boolean autoCommit);

    public MySQLIsolation getIsolation();

    public void setIsolation(MySQLIsolation isolation);

    public boolean isInTransaction();

    public void setInTransaction(boolean inTransaction);

    public MycatUser getUser();

    void setUser(MycatUser user);

    void useShcema(String schema);

    String getDefaultSchema();

    int getServerCapabilities();

    void setServerCapabilities(int serverCapabilities);

    int getWarningCount();

    long getLastInsertId();

    void setLastInsertId(long s);

    Charset getCharset();

    int getCharsetIndex();

    int getLastErrorCode();

    long getAffectedRows();

    void setAffectedRows(long affectedRows);

    String getLastMessage();

    void setLastMessage(String lastMessage);

    void setCharset(int index, String charsetName, Charset defaultCharset);

    AtomicBoolean getCancelFlag();

    default boolean isRunning() {
        return !getCancelFlag().get();
    }

    boolean isReadOnly();

    void close();

    //need catch exception
//    void block(Runnable runnable);

    public String resolveDatasourceTargetName(String targetName);

    public String resolveDatasourceTargetName(String targetName, boolean master);

    Map<Long, PreparedStatement> getPrepareInfo();

    SqlRecord startSqlRecord();

    SqlRecord currentSqlRecord();

    void endSqlRecord();



    default String setLastMessage(Throwable e) {
        LOGGER.error("",e);
        String string = getThrowableString(e);
        setLastMessage(string);
        return string;
    }

    static String getThrowableString(Throwable e) {
        StringWriter errors = new StringWriter();
        e.printStackTrace(new PrintWriter(errors));
        //去掉重复提示的消息 return MessageFormat.format("{0} \n {1}",e,errors.toString());
        return errors.toString();
    }

    public long nextPrepareStatementId();

    void setCharsetIndex(int characterSet);

    void setLastErrorCode(int errorCode);

    Map<String,Object> getProcessStateMap();
    void putProcessStateMap(Map<String, Object> map);
}