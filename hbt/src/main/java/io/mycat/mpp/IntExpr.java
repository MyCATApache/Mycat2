package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.SQLObject;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.Type;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class IntExpr implements SqlValue {
    final int value;

    public static IntExpr of(int value) {
        return new IntExpr(value);
    }

    @Override
    public Object getValue(Type type, DataAccessor dataAccessor, DataContext context) {
        return value;
    }

    @Override
    public boolean getValueAsBoolean(Type columns, DataAccessor dataAccessor, DataContext dataContext) {
        return false;
    }

    @Override
    public SQLObject toParseTree() {
        return null;
    }
}