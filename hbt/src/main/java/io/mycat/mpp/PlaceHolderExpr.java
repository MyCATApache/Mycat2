package io.mycat.mpp;


import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.RowType;
import io.mycat.mpp.runtime.Type;

public class PlaceHolderExpr implements SqlValue {
    final String name;
    private Type type;

    public PlaceHolderExpr(String name, Type type) {
        this.name = name;
        this.type = type;
    }

    public static PlaceHolderExpr create(String name,Type type) {
        return new PlaceHolderExpr(name,type);
    }

    @Override
    public Object getValue(RowType type, DataAccessor dataAccessor, DataContext context) {
        return context.getVariantRefExpr(name);
    }

    @Override
    public Type getType() {
        return Type.of(Type.VARCHAR, true);
    }

    @Override
    public SQLObject toParseTree() {
        return new SQLIdentifierExpr(name);
    }
}