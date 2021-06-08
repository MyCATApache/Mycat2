package io.mycat.querycondition;

import io.mycat.Partition;

import java.util.List;

public abstract class PredicateQuery {
    private PredicateType type;

    public PredicateQuery(PredicateType type) {
        this.type = type;
    }

    public PredicateType type() {
        return type;
    }

    public abstract List<Partition> apply(Object[] params);
}
