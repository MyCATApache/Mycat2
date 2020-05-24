package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.SQLObject;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.Type;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class AccessDataExpr implements SqlValue {
    int index;

    public static final AccessDataExpr of(int index) {
        return new AccessDataExpr(index);
    }

    @Override
    public SQLObject toParseTree() {
        return null;
    }


    @Override
    public Object getValue(Type type, DataAccessor dataAccessor, DataContext context) {
        return null;
    }

    @Override
    public boolean getValueAsBoolean(Type columns, DataAccessor dataAccessor, DataContext dataContext) {
        return false;
    }
}