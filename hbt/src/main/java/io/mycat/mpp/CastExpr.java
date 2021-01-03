package io.mycat.mpp;

import com.alibaba.druid.sql.ast.SQLObject;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.RowType;
import io.mycat.mpp.runtime.Type;

import java.util.function.Function;

public class CastExpr implements SqlValue {
    final SqlValue expr;
    final Type targetType;
    final Type originType;
    final Function cast;

    public CastExpr(SqlValue expr, Type targetType) {
        this.expr = expr;
        this.targetType = targetType;
        this.originType = expr.getType();
        this.cast = TypeSystem.convert(originType, targetType);
    }

    @Override
    public Object getValue(RowType type, DataAccessor dataAccessor, DataContext context) {
        return cast.apply(expr.getValue(type, dataAccessor, context));
    }

    @Override
    public boolean getValueAsBoolean(RowType columns, DataAccessor dataAccessor, DataContext dataContext) {
        return false;
    }

    @Override
    public Type getType() {
        return targetType;
    }

    @Override
    public SQLObject toParseTree() {
        return null;
    }
}