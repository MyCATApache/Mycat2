package io.mycat.calcite.spm;

import lombok.*;

import java.util.concurrent.atomic.AtomicReference;


@EqualsAndHashCode
@Data
@ToString
public class BaselinePlan<T> {
    final String sql;
    final String rel;
    final long id;
    final long baselineId;
    final boolean accept;

     T attach;

    public BaselinePlan(String sql, String rel, long id, long baselineId, T attach) {
        this(sql, rel, id, baselineId, false, (attach));
    }

    public BaselinePlan(String sql, String rel, long id, long baselineId, boolean accept, T attach) {
        this.sql = sql;
        this.rel = rel;
        this.id = id;
        this.baselineId = baselineId;
        this.accept = true;
        this.attach = attach;
    }

    public T getAttach() {
        return attach;
    }
}