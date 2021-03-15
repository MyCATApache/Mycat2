package io.mycat.connectionschedule;


import io.vertx.core.Promise;
import io.vertx.sqlclient.SqlConnection;
import lombok.Getter;

import java.util.Comparator;
import java.util.function.ToIntFunction;

@Getter
public class SubTask implements Comparable<SubTask> {
        final int order;
        final String target;
        final int count;
        final Promise<SqlConnection> promise;
        final long deadline;
        static final Comparator<SubTask> subTaskComparator = Comparator.comparingInt((ToIntFunction<SubTask>) value -> value.order)
                .thenComparing((Comparator.comparingInt((ToIntFunction<SubTask>) value -> value.count).reversed()));

        public SubTask(int order, int count, String target, Promise<SqlConnection> promise, long deadline) {
            this.order = order;
            this.target = target;
            this.count = count;
            this.promise = promise;
            this.deadline = deadline;
        }

        @Override
        public int compareTo( SubTask o) {
            return subTaskComparator.compare(this, o);
        }
    }