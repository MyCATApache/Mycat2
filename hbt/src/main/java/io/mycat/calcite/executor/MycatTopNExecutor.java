/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite.executor;

import io.mycat.calcite.Executor;
import io.mycat.calcite.ExplainWriter;
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

    protected MycatTopNExecutor(Comparator<Row> comparator, long offset, long fetch, Executor executor) {
        this.comparator = comparator;
        this.offset = offset;
        this.fetch = fetch;
        this.executor = executor;
        this.size = (int) (offset + fetch);
        this.queue = new PriorityQueue<>(this.size, comparator);
    }
    public static MycatTopNExecutor create(Comparator<Row> comparator, long offset, long fetch, Executor executor) {
        return new MycatTopNExecutor(
                comparator,
                offset,
                fetch,
                executor
        );
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

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        ExplainWriter explainWriter = writer.name(this.getClass().getName())
                .into();
        writer.item("offset",offset);
        writer.item("fetch",fetch);
        executor.explain(writer);
        return explainWriter.ret();
    }
}