package io.mycat.mpp;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.RowType;
import io.mycat.mpp.runtime.Type;

public class BetweenNode implements SqlValue {
    final SqlValue testExpr;
    final SqlValue begin;
    final SqlValue end;

    protected BetweenNode(SqlValue testExpr, SqlValue begin, SqlValue end) {
        this.testExpr = testExpr;
        this.begin = begin;
        this.end = end;
    }

    public static BetweenNode create(SqlValue testExpr, SqlValue begin, SqlValue end) {
        return new BetweenNode(testExpr, begin, end);
    }

    @Override
    public SQLObject toParseTree() {
        return new SQLBetweenExpr(
                (SQLExpr) testExpr.toParseTree(),
                (SQLExpr) begin.toParseTree(),
                (SQLExpr) end.toParseTree()
        );
    }

    @Override
    public Object getValue(RowType type, DataAccessor dataAccessor, DataContext context) {
        return getValueAsBoolean(type, dataAccessor, context) ? 1 : 0;
    }

    @Override
    public boolean getValueAsBoolean(RowType columns, DataAccessor dataAccessor, DataContext dataContext) {
        Comparable begin = (Comparable) this.begin.getValue(columns, dataAccessor, dataContext);
        Comparable end = (Comparable) this.end.getValue(columns, dataAccessor, dataContext);
        Comparable value = (Comparable) this.testExpr.getValue(columns, dataAccessor, dataContext);
        return begin.compareTo(value) <= 0 && end.compareTo(value) >= 0;
    }

    @Override
    public Type getType() {
        return Type.of(Type.INT, false);
    }
}