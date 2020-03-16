package io.mycat.beans;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DatabaseClientStateApiDecorator implements DatabaseClientStateApi {
    final DatabaseClientStateApi clientStateApi;

    @Override
    public String getSchema() {
        return clientStateApi.getSchema();
    }

    @Override
    public void begin() {
        clientStateApi.begin();
    }

    @Override
    public void rollback() {
        clientStateApi.rollback();
    }

    @Override
    public void useSchema(String normalize) {
        clientStateApi.useSchema(normalize);
    }

    @Override
    public void commit() {
        clientStateApi.commit();
    }

    @Override
    public void setTransactionIsolation(int value) {
        clientStateApi.setTransactionIsolation(value);
    }

    @Override
    public int getTransactionIsolation() {
        return clientStateApi.getTransactionIsolation();
    }

    @Override
    public boolean isAutocommit() {
        return clientStateApi.isAutocommit();
    }

    @Override
    public long getMaxRow() {
        return clientStateApi.getMaxRow();
    }

    @Override
    public void setMaxRow(long value) {
        clientStateApi.setMaxRow(value);
    }

    @Override
    public void setAutocommit(boolean autocommit) {
        clientStateApi.setAutocommit(autocommit);
    }
}