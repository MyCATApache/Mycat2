package io.mycat.upondb;

import io.mycat.MycatConnection;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public interface MycatDBClientBased {

    MycatDBSharedServer getUponDBSharedServer();

    MycatDBClientBasedConfig config();

    Map<String, Object> variables();

    MycatConnection getConnection(String targetName);

    void begin();

    void rollback();

    void commit();

    void setTransactionIsolation(int value);

    int getTransactionIsolation();

    boolean isAutocommit();

    void setAutocommit(boolean autocommit);

    void close();

    AtomicBoolean cancelFlag();

   String resolveFinalTargetName(String targetName);

    void addCloseResource(AutoCloseable connection);
}