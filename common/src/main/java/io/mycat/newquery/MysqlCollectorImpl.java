package io.mycat.newquery;

import io.mycat.beans.mycat.MycatRowMetaData;

public class MysqlCollectorImpl implements MysqlCollector {
    MycatRowMetaData mycatRowMetaData;

    @Override
    public void onColumnDef(MycatRowMetaData mycatRowMetaData) {
        this.mycatRowMetaData = mycatRowMetaData;
    }

    @Override
    public void onRow(Object[] row) {

    }

    @Override
    public void onComplete() {

    }

    @Override
    public void onError(Throwable e) {

    }
}
