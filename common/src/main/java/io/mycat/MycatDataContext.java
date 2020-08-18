package io.mycat;

import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLServerStatusFlags;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public interface MycatDataContext extends Wrapper, SessionOpt {

    TransactionType transactionType();

    TransactionSession getTransactionSession();


    void setTransactionSession(TransactionSession transactionSession);

    void switchTransaction(TransactionType transactionSessionType);

    default <T> T getVariable(String target) {
        return (T) MySQLVariablesUtil.getVariable(this, target);
    }

    <T> T getVariable(MycatDataContextEnum name);

    void setVariable(MycatDataContextEnum name, Object value);

    default void setVariable(String target,
                             Object text) {
        MySQLVariablesUtil.setVariable(this, target, text);
    }

    default int serverStatus() {
        MySQLServerStatusFlags.Builder builder = MySQLServerStatusFlags.builder();
        if (getVariable(MycatDataContextEnum.IS_AUTOCOMMIT)) {
            builder.setAutoCommit();
        }
        if (getVariable(MycatDataContextEnum.IS_IN_TRANSCATION)) {
            builder.setInTransaction();
        }
        if (getVariable(MycatDataContextEnum.IS_READ_ONLY)) {
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

    void useShcema(String schema);

    void setUser(MycatUser user);

    String getDefaultSchema();

    int getServerCapabilities();

    int getWarningCount();

    long getLastInsertId();

    Charset getCharset();

    int getCharsetIndex();

    void setLastInsertId(long s);

    int getLastErrorCode();

    long getAffectedRows();

    void setLastMessage(String lastMessage);

    String getLastMessage();

    void setServerCapabilities(int serverCapabilities);

    void setAffectedRows(long affectedRows);

    void setCharset(int index, String charsetName, Charset defaultCharset);

    AtomicBoolean getCancelFlag();

    default boolean isRunning() {
        return !getCancelFlag().get();
    }

    void run(Runnable runnable);

    boolean isReadOnly();

    @Override
    default boolean continueBindThreadIfTransactionNeed() {
        return isInTransaction();
    }

    void close();

    //need catch exception
    void block(Runnable runnable);

    public String resolveDatasourceTargetName(String targetName);

    Map<Long, PreparedStatement> getPrepareInfo();
}