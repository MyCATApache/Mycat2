package io.mycat.api.collector;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observer;

import java.io.IOException;

public class SimpleRowObservable extends RowObservable {
    final MycatRowMetaData metaData;
    final io.reactivex.rxjava3.core.Observable<Object[]> observable;

    public SimpleRowObservable(MycatRowMetaData metaData,io.reactivex.rxjava3.core.Observable<Object[]> observable) {
        this.metaData = metaData;
        this.observable = observable;
    }

    public static RowObservable of(MycatRowMetaData metaData,Iterable<Object[]> source){
        return new SimpleRowObservable(metaData,io.reactivex.rxjava3.core.Observable.fromIterable(source));
    }

    @Override
    public MycatRowMetaData getRowMetaData() {
        return metaData;
    }

    @Override
    protected void subscribeActual(@NonNull Observer<? super Object[]> observer) {
        observable.subscribe(observer);
    }
}
