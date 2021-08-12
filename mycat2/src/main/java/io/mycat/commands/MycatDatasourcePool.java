package io.mycat.commands;

import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;

public interface MycatDatasourcePool {
    public abstract Future<SqlConnection> getConnection();

    public abstract Integer getAvailableNumber();
    public abstract Integer getUsedNumber();
    public String getTargetName();

}
