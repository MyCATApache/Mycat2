package io.mycat.beans;

import java.util.HashMap;
import java.util.Map;

public abstract class DefaultDatabaseClientStateTeamplate implements DatabaseClientStateApi {
    private String schema;
    private long maxRow;
    private final Map<String, Object> varbables = new HashMap<>();

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public void begin() {

    }

    @Override
    public void rollback() {

    }

    @Override
    public void useSchema(String normalize) {
        schema = normalize;
    }

    @Override
    public void commit() {

    }

    @Override
    public void setTransactionIsolation(int value) {

    }

    @Override
    public long getMaxRow() {
        return 0;
    }

    @Override
    public void setMaxRow(long value) {
        this.maxRow = value;
    }

    @Override
    public void setAutocommit(boolean autocommit) {

    }
}