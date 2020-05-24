package io.mycat.mpp.plan;

import io.mycat.mpp.DataContext;

import java.util.ArrayList;
import java.util.List;

public class UnionPlan extends ColumnThroughPlan {
    final List<QueryPlan> froms;

    public UnionPlan(QueryPlan from, List<QueryPlan> others) {
        super(from);
        this.froms = new ArrayList<>(1 + others.size());
        this.froms.add(from);
        this.froms.addAll(others);
    }

    @Override
    public Scanner scan(DataContext dataContext, long flags) {
        return Scanner.of(this.froms.stream().map(i -> i.scan(dataContext, flags))
                .flatMap(i -> i.stream()));
    }
}