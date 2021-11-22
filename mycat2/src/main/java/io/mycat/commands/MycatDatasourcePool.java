package io.mycat.commands;

import io.mycat.newquery.NewMycatConnection;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;

public interface MycatDatasourcePool {
    public abstract Future<NewMycatConnection> getConnection();

    public abstract Integer getAvailableNumber();
    public abstract Integer getUsedNumber();
    public String getTargetName();
    public void close();

}
