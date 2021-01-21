package io.mycat.api.collector;

import io.mycat.beans.mycat.MycatRowMetaData;

public abstract class RowObservable extends io.reactivex.rxjava3.core.Observable<Object[]> {
    public abstract MycatRowMetaData getRowMetaData();
}
