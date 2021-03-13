package io.mycat.commands;

import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public  abstract class MycatDatasourcePool {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatDatasourcePool.class);
    protected final String targetName;

    public MycatDatasourcePool(String targetName) {
        this.targetName = targetName;
    }

    public abstract Future<SqlConnection> getConnection();

    public abstract Integer getAvailableNumber();
    public abstract Integer getUsedNumber();
    public String getTargetName() {
        return targetName;
    }
}
