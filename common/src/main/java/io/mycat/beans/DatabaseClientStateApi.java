package io.mycat.beans;

public interface DatabaseClientStateApi {

    String getSchema();

    void begin();

    void rollback();

    void useSchema(String normalize);

    void commit();

    void setTransactionIsolation(int value);

    int getTransactionIsolation();

    boolean isAutocommit();

    long getMaxRow();

    void setMaxRow(long value);

    void setAutocommit(boolean autocommit);
}