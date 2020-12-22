package io.mycat.beans.resultset;

import io.mycat.ExecuteType;
import lombok.Getter;

import java.io.IOException;


@Getter
public class MycatProxyResponse implements MycatResponse {
    final ExecuteType executeType;
    final String targetName;
    final String sql;

    public MycatProxyResponse(ExecuteType executeType,
                              String targetName,
                              String sql) {
        this.executeType = executeType;
        this.targetName = targetName;
        this.sql = sql;
    }

    public static MycatProxyResponse create(ExecuteType executeType, String targetName, String sql) {
        return new MycatProxyResponse(executeType, targetName, sql);
    }

    @Override
    public MycatResultSetType getType() {
        return MycatResultSetType.PROXY;
    }

    @Override
    public void close() throws IOException {

    }
}