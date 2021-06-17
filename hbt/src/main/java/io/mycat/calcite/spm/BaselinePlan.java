package io.mycat.calcite.spm;

import io.mycat.MetaClusterCurrent;
import lombok.*;

import java.util.Objects;


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

    public T attach() {
        if (attach != null) {
            return attach;
        }
        MemPlanCache memPlanCache = MetaClusterCurrent.wrapper(MemPlanCache.class);
        attach = (T) memPlanCache.getCodeExecuterContext(memPlanCache.getBaseline(baselineId),this);
        return (T) attach;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaselinePlan<?> that = (BaselinePlan<?>) o;
        return id == that.id && baselineId == that.baselineId && accept == that.accept && Objects.equals(sql, that.sql) && Objects.equals(rel, that.rel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sql, rel, id, baselineId, accept);
    }
}