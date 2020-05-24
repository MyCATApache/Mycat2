package io.mycat.mpp.plan;

import io.mycat.mpp.DataContext;
import io.mycat.mpp.SqlValue;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;

public class NestedLoopPlan extends NodePlan {
    private final QueryPlan left;
    private final QueryPlan right;
    private final JoinType joinRelType;
    private final SqlValue condition;

    public NestedLoopPlan(QueryPlan from, QueryPlan left, QueryPlan right, JoinType joinRelType, SqlValue condition) {
        super(from);
        this.left = left;
        this.right = right;
        this.joinRelType = joinRelType;
        this.condition = condition;
    }

    @Override
    public Type getColumns() {
        return null;
    }

    @Override
    public Scanner scan(DataContext dataContext, long flags) {
        final Enumerable<DataAccessor> outer = Linq4j.asEnumerable(() -> left.scan(dataContext, flags));
        final Enumerable<DataAccessor> inner = Linq4j.asEnumerable(() -> right.scan(dataContext, flags));
        final Predicate2<DataAccessor, DataAccessor> predicate = null;
        Function2<DataAccessor, DataAccessor, DataAccessor> resultSelector = null;
        final JoinType joinType = this.joinRelType;
        return Scanner.of(EnumerableDefaults.nestedLoopJoin(outer, inner, predicate, resultSelector, joinType).iterator());
    }
}