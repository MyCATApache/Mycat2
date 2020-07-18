package io.mycat.hbt4.executor;

import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

public class MycatTopNExecutor implements Executor {
    private final Comparator<Row> comparator;
    private final long offset;
    private final long fetch;
    private final Executor executor;
    private final PriorityQueue<Row> queue;
    private final int size;
    private Iterator<Row> iterator;

    public MycatTopNExecutor(Comparator<Row> comparator, long offset, long fetch, Executor executor) {
        this.comparator = comparator;
        this.offset = offset;
        this.fetch = fetch;
        this.executor = executor;
        this.size = (int) (offset + fetch);
        this.queue = new PriorityQueue<>(this.size, comparator);
    }

    @Override
    public void open() {
        if (iterator == null) {
            executor.open();
            Iterator<Row> iterator = executor.iterator();
            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (queue.size() < this.size) {
                    queue.add(row);
                } else {
                    if (comparator.compare(row, queue.peek()) < 0) {
                        queue.poll();
                        queue.add(row);
                    }
                }
            }
            for (int i = 0; i < offset && !queue.isEmpty(); i++) {
                queue.poll();
            }
            executor.close();
        }
        this.iterator = queue.iterator();
    }

    @Override
    public Row next() {
        if (this.iterator.hasNext()) {
            return this.iterator.next();
        }
        return null;
    }

    @Override
    public void close() {
        executor.close();
    }

    @Override
    public boolean isRewindSupported() {
        return true;
    }
}