package io.mycat;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.disposables.Disposable;

public abstract class MycatResultSetObserver implements io.reactivex.rxjava3.core.Observer<Object[]> {
    final MycatRowMetaData rowMetaData;
    final Response response;
    final  int columnCount;
    Disposable disposable;

    public MycatResultSetObserver(MycatRowMetaData rowMetaData,Response response) {
        this.response = response;
        this.rowMetaData = rowMetaData;
        this. columnCount = rowMetaData.getColumnCount();
    }

    @Override
    public void onSubscribe(@NonNull Disposable d) {
        disposable = d;
    }

    @Override
    public void onError(@NonNull Throwable e) {
        response.sendError(e);
    }

    @Override
    public void onComplete() {
        response.sendOk();
    }
}
