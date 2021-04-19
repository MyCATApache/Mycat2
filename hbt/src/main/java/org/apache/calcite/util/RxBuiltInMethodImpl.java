package org.apache.calcite.util;


import hu.akarnokd.rxjava3.operators.Flowables;
import io.mycat.serializable.MaterializedRecordSetFactory;
import io.mycat.serializable.OffHeapObjectList;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Predicate1;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class RxBuiltInMethodImpl {

    public static Observable<Object[]> select(Observable<Object[]> input, org.apache.calcite.linq4j.function.Function1<Object[], Object[]> map) {
        return input.map(objects -> map.apply(objects));
    }

    public static Observable<Object[]> filter(Observable<Object[]> input, Predicate1<Object[]> filter) {
        return input.filter(objects -> filter.apply(objects));
    }


    public static Observable<Object[]> unionAll(List<Observable<Object[]>> inputs) {
        return Observable.merge(inputs);
    }

    public static Observable<Object[]> unionAll(Observable<Object[]> left, Observable<Object[]> right) {
        return Observable.merge(left, right);
    }

    public static Observable<Object[]> topN(Observable<Object[]> input, Comparator<Object[]> sortFunction, long skip, long limit) {
        return input.sorted(sortFunction).skip(skip).take(limit);
    }

    public static Observable<Object[]> sort(Observable<Object[]> input, Comparator<Object[]> sortFunction) {
        return input.sorted(sortFunction);
    }

    public static Observable<Object[]> limit(Observable<Object[]> input, long limit) {
        return input.take(limit);
    }

    public static Observable<Object[]> offset(Observable<Object[]> input, long limit) {
        return input.skip(limit);
    }

    public static Enumerable<Object[]> toEnumerable(Object input) {
        if (input instanceof Observable) {
            return Linq4j.asEnumerable(((Observable) input).cache().blockingIterable());
        }
        return (Enumerable<Object[]>) input;
    }

    public static Observable<Object[]> toObservable(Object input) {
        return Observable.fromIterable((Enumerable) input);
    }

    public static <T> Observable<T> mergeSort(List<Observable<T>> inputs,
                                              Comparator<T> sortFunction,
                                              long skip, long limit) {
        return mergeSort(inputs, sortFunction).skip(skip).take(limit);
    }

    public static <T> Observable<T> mergeSort(List<Observable<T>> inputs, Comparator<T> sortFunction) {
        Flowable<T> flowable = Flowables.orderedMerge(inputs.stream().map(i -> i.toFlowable(BackpressureStrategy.BUFFER)).collect(Collectors.toList()),
                sortFunction);
        return flowable.toObservable();
    }

    public static Enumerable<Object[]> matierial(Enumerable<Object[]> input) {
        return new AbstractEnumerable<Object[]>() {

            @Override
            public Enumerator<Object[]> enumerator() {
                OffHeapObjectList recordSet = MaterializedRecordSetFactory.DEFAULT_FACTORY.createRecordSet();
                Enumerator<Object[]> enumerator = input.enumerator();
                while (enumerator.moveNext()) {
                    recordSet.addObjects(enumerator.current());
                }
                recordSet.finish();
                enumerator.close();
                return new Enumerator<Object[]>() {
                    Enumerator<Object[]> iterator = Linq4j.iterableEnumerator(recordSet);

                    @Override
                    public Object[] current() {
                        return iterator.current();
                    }

                    @Override
                    public boolean moveNext() {
                        return iterator.moveNext();
                    }

                    @Override
                    public void reset() {
                        iterator = Linq4j.iterableEnumerator(recordSet);
                    }

                    @Override
                    public void close() {
                        recordSet.close();
                    }
                };
            }
        };
    }


    public static Observable<Object[]> asObservable(Object[][] input) {
        return Observable.fromArray(input);
    }

    public static Observable<Object[]> asObservable(Object[] input) {
        return Observable.fromIterable(() -> Arrays.stream(input).map(i -> (Object[]) i).iterator());
    }

    public static Enumerable<Object[]> asGather(Object input) {
        if (input instanceof Enumerable) {
            Enumerable input1 = (Enumerable) input;
            return Linq4j.asEnumerable(() -> StreamSupport.stream(input1.spliterator(), true).iterator());
        } else {
            return asGather(toEnumerable(input));
        }
    }
}
