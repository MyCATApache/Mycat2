package io.mycat.mpp;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.RowType;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Type;
import lombok.SneakyThrows;

import java.util.Objects;


public abstract class BinaryOp  implements SqlValue {
    final SqlValue leftExpr;
    final SqlValue rightExpr;
    final Type returnType;
    final SQLBinaryOperator operator;
    final Invoker fun;

    public BinaryOp(SQLBinaryOperator operator,SqlValue leftExpr, SqlValue rightExpr, Type returnType,  Invoker fun) {
        this.leftExpr = leftExpr;
        this.rightExpr = rightExpr;
        this.returnType = returnType;
        this.operator = operator;
        this.fun = Objects.requireNonNull(fun);
    }

    @Override
    public SQLObject toParseTree() {
        return SQLUtils.buildCondition(operator, (SQLExpr) leftExpr.toParseTree(), true, (SQLExpr) rightExpr.toParseTree());
    }

    @Override
    @SneakyThrows
    public Object getValue(RowType type, DataAccessor dataAccessor, DataContext context) {
        return fun.invokeWithArguments(leftExpr.getValue(type, dataAccessor, context), rightExpr.getValue(type, dataAccessor, context));
    }

    @Override
    public Type getType() {
        return returnType;
    }
}
