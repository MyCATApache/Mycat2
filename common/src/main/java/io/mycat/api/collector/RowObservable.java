package io.mycat.api.collector;

import io.mycat.beans.mycat.MycatRowMetaData;

public abstract class RowObservable extends io.reactivex.rxjava3.core.Observable<Object[]> {
    protected MycatRowMetaData metaData;

    public MycatRowMetaData getRowMetaData() {
        return metaData;
    }

    public void setRowMetaData(MycatRowMetaData metaData) {
        this.metaData = metaData;
    }
}
