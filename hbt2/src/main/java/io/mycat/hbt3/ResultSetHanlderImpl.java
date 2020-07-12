package io.mycat.hbt3;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.mpp.Row;

public class ResultSetHanlderImpl implements ResultSetHanlder {
    @Override
    public void onOk() {

    }

    @Override
    public void onMetadata(MycatRowMetaData mycatRowMetaData) {

    }

    @Override
    public void onRow(Row row) {
        System.out.println(row);
    }

    @Override
    public void onError(Throwable e) {

    }
}