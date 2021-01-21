package io.mycat.api.collector;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.resultset.MycatObserverResponse;

public abstract class RowObservable extends io.reactivex.rxjava3.core.Observable<Object[]> implements MycatObserverResponse {
    public abstract MycatRowMetaData getRowMetaData();
}
