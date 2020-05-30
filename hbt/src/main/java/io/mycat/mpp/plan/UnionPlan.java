package io.mycat.mpp.plan;

import io.mycat.mpp.DataContext;

import java.util.ArrayList;
import java.util.List;

public class UnionPlan extends ColumnThroughPlan {
    final List<QueryPlan> froms;
    private final boolean concurrent;

    public UnionPlan(QueryPlan from, List<QueryPlan> others, boolean concurrent) {
        super(from);
        this.froms = new ArrayList<>(1 + others.size());
        this.concurrent = concurrent;
        this.froms.add(from);
        this.froms.addAll(others);
    }

    public UnionPlan(List<QueryPlan> froms, boolean concurrent) {
        super(froms.get(0));
        this.froms = froms;
        this.concurrent = concurrent;
    }

    public static UnionPlan create(List<QueryPlan> asList) {
        return create(asList, false);
    }

    public static UnionPlan create(List<QueryPlan> asList, boolean concurrent) {
        return new UnionPlan(asList, concurrent);
    }

    @Override
    public Scanner scan(DataContext dataContext, long flags) {
        return Scanner.of(this.froms.stream().map(i -> i.scan(dataContext, flags))
                .flatMap(i -> i.stream()));
    }
}