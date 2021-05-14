package io.mycat.querycondition;

import io.mycat.DataNode;

import java.util.List;

public abstract class PredicateQuery {
    private PredicateType type;

    public PredicateQuery(PredicateType type) {
        this.type = type;
    }

    public PredicateType type() {
        return type;
    }

    public abstract List<DataNode> apply(Object[] params);
}
