package org.apache.calcite.util;


import hu.akarnokd.rxjava3.operators.Flowables;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import kotlin.jvm.functions.Function1;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Predicate1;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class RxBuiltInMethodImpl {

    public static   Observable<Object[]> select(Observable<Object[]> input, Function1<Object[], Object[]> map) {
        return input.map(objects -> map.invoke(objects));
    }

    public static  Observable<Object[]> filter(Observable<Object[]> input, Predicate1<Object[]> filter) {
        return input.filter(objects -> filter.apply(objects));
    }

    public static Observable<Object[]> calc(Observable<Object[]> input, Function1<Object[], Object[]> map) {
        return select(input, map);
    }

    public static Observable<Object[]> unionAll(List<Observable<Object[]>> inputs) {
        return inputs.stream().reduce((observable, observable2) -> observable.concatWith(observable2)).orElse(Observable.empty());
    }
    public static Observable<Object[]> unionAll(Observable<Object[]> left,Observable<Object[]> right) {
        return left.concatWith(right);
    }
    public static  Observable<Object[]> topN(Observable<Object[]> input, Comparator<Object[]> sortFunction, long skip, long limit) {
        return input.sorted(sortFunction).skip(skip).take(limit);
    }

    public static Observable<Object[]> sort(Observable<Object[]> input, Comparator<Object[]> sortFunction) {
        return input.sorted(sortFunction);
    }

    public static Observable<Object[]> limit(Observable<Object[]> input, long limit) {
        return input.take(limit);
    }

    public static Observable<Object[]> offset(Observable<Object[]> input, long limit) {
        return input.take(limit);
    }

    public static Enumerable<Object[]> observableToEnumerable(Observable<Object[]> input) {
        return Linq4j.asEnumerable(input.blockingIterable());
    }

    public static Observable<Object[]> enumerableToObservable(Enumerable<Object[]> input) {
        return Observable.fromIterable(input);
    }

    public static <T> Observable<T> mergeSort(List<Observable<T>> inputs,
                                                 Comparator<T> sortFunction,
                                                 long skip, long limit) {
        return mergeSort(inputs, sortFunction).skip(skip).take(limit);
    }

    public static <T>  Observable<T> mergeSort(List<Observable<T>> inputs, Comparator<T> sortFunction) {
        Flowable<T> flowable = Flowables.orderedMerge(inputs.stream().map(i -> i.toFlowable(BackpressureStrategy.BUFFER)).collect(Collectors.toList()),
                sortFunction);
        return flowable.toObservable();
    }

    public static Observable<Object[]> matierial(Observable<Object[]> input) {
        return input.cache();
    }

    public static Observable<Object[]> asObservable(Object[][] input) {
        return Observable.fromArray(input);
    }
}
