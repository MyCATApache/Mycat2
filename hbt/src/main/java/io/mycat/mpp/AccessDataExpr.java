package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.SQLObject;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.RowType;
import io.mycat.mpp.runtime.Type;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class AccessDataExpr implements SqlValue {
   final int index;
    final Type mySQLType;

    public static final AccessDataExpr of(int index, Type mySQLType) {
        return new AccessDataExpr(index, mySQLType);
    }

    @Override
    public SQLObject toParseTree() {
        return null;
    }


    @Override
    public Object getValue(RowType type, DataAccessor dataAccessor, DataContext context) {
        return dataAccessor.get(index);
    }

    @Override
    public boolean getValueAsBoolean(RowType columns, DataAccessor dataAccessor, DataContext dataContext) {
        return dataAccessor.get(index) != TypeSystem.NULL;
    }

    @Override
    public Type getType() {
        return mySQLType;
    }
}