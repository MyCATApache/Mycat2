package io.mycat.mpp.plan;

import io.mycat.mpp.DataContext;
import io.mycat.mpp.JoinKey;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;

public class HashJoinPlan extends NodePlan {
    private final QueryPlan left;
    private final QueryPlan right;
    private final int[] leftKeys;
    private final int[] rightKeys;
    private final boolean generateNullsOnLeft;
    private final boolean generateNullsOnRight;

    public HashJoinPlan(QueryPlan left, QueryPlan right, int[] leftKeys, int[] rightKeys, boolean generateNullsOnLeft, boolean generateNullsOnRight) {
        super(left);
        this.left = left;
        this.right = right;
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.generateNullsOnLeft = generateNullsOnLeft;
        this.generateNullsOnRight = generateNullsOnRight;
    }

    @Override
    public Type getColumns() {
        return null;
    }

    @Override
    public Scanner scan(DataContext dataContext, long flags) {
        Scanner left = this.left.scan(dataContext, flags);
        Scanner right = this.right.scan(dataContext, flags);

        Enumerable<DataAccessor> outer = Linq4j.asEnumerable(() -> left);
        Enumerable<DataAccessor> inner = Linq4j.asEnumerable(() -> right);
        Function1<DataAccessor, JoinKey> leftJoinKey = a0 ->  JoinKey.keyExtractor(leftKeys).apply(a0);
        Function1<DataAccessor, JoinKey> rightJoinKey = a0 -> JoinKey.keyExtractor(rightKeys).apply(a0);
        Function2<DataAccessor, DataAccessor, DataAccessor> resultSelector = new Function2<DataAccessor, DataAccessor, DataAccessor>() {
            @Override
            public DataAccessor apply(DataAccessor v0, DataAccessor v1) {
                return null;
            }
        };
        return Scanner.of(EnumerableDefaults.hashJoin(outer,
                inner,
                leftJoinKey,
                rightJoinKey,
                resultSelector,
                null,generateNullsOnLeft,generateNullsOnRight).iterator());
    }
}