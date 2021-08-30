package io.mycat.newquery;

import io.mycat.beans.mycat.MycatRowMetaData;

import java.sql.SQLException;

public interface MysqlCollector {
    void onColumnDef(MycatRowMetaData mycatRowMetaData);
    void onRow(Object[] row);
    void onComplete();
    void onError(Exception e);
}
