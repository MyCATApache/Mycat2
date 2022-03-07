package io.mycat;

import io.reactivex.rxjava3.core.Observable;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class MycatRxJavaUtl {
    public static <T> Iterable<T> blockingIterable(Observable<T> observable){
        return observable.toList().timeout(5, TimeUnit.MINUTES).blockingGet();
    }
}
