package io.mycat.hbt4;

import io.mycat.mpp.Row;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.stream.StreamSupport;

public class MycatMergeSortExecutor implements Executor {
    private final Comparator<Row> comparator;
    private final long offsetValue;
    private final long fetchValue;
    private final Executor[] executors;
    private final Iterator[] iterators;
    private final PriorityQueue<Row> queue;
    Iterator<Row> iterator;

    public MycatMergeSortExecutor(Comparator<Row> comparator, long offsetValue, long fetchValue, Executor[] executors) {
        this.comparator = comparator;
        this.offsetValue = offsetValue;
        this.fetchValue = fetchValue;
        this.executors = executors;
        this.iterators = new Iterator[executors.length];
        this.queue = new PriorityQueue<>((int) (offsetValue + fetchValue), this.comparator);
    }

    @Override
    public void open() {
        for (int i = 0; i < executors.length; i++) {
            Executor executor = executors[i];
            executor.open();
            iterators[i] = executor.iterator();
        }
        Iterable<Row> iterable = () -> new Iterator<Row>() {
            @Override
            public boolean hasNext() {
                for (int i = 0; i < executors.length; i++) {
                    Iterator<Row> iterator = iterators[i];
                    if (iterator != null) {
                        if (iterator.hasNext()) {
                            queue.add(iterator.next());
                        } else {
                            executors[i].close();
                        }
                    }
                }
                return !queue.isEmpty();
            }

            @Override
            public Row next() {
                return queue.poll();
            }
        };
        iterator = StreamSupport.stream(iterable.spliterator(), false).skip(offsetValue).limit(fetchValue).iterator();
    }

    @Override
    public Row next() {
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            return null;
        }
    }


    @Override
    public void close() {
        for (Executor executor : executors) {
            executor.close();
        }

    }

    @Override
    public boolean isRewindSupported() {
        return false;
    }
}