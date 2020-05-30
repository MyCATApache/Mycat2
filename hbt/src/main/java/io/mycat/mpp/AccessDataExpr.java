package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.RowType;
import io.mycat.mpp.runtime.Type;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class AccessDataExpr implements SqlValue {
   final int index;
    final Type mySQLType;
    final String columnName;

    public static final AccessDataExpr of(int index,Type mySQLType,String columnName) {
        return new AccessDataExpr(index,mySQLType,columnName);
    }

    @Override
    public SQLObject toParseTree() {
        return new SQLIdentifierExpr(columnName);
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