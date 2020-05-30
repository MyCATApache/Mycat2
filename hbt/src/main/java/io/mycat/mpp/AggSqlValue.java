package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLAggregateOption;
import io.mycat.mpp.runtime.Type;
import lombok.Getter;

import java.util.List;

@Getter
public class AggSqlValue implements SqlValue {
  final String name;
    final List<SqlValue> args;

    public AggSqlValue(String name, List<SqlValue> args) {
        this.name = name;
        this.args = args;
    }

    public static AggSqlValue of(String name, List<SqlValue> args){
        return new AggSqlValue(name, args);
    }
    @Override
    public Type getType() {
        return Type.getFromClass(AggCalls.getReturnType(name));
    }

    @Override
    public SQLObject toParseTree() {
        SQLAggregateExpr sqlAggregateExpr = new SQLAggregateExpr(name, SQLAggregateOption.ALL);
        int size = args.size();
        for (int i = 0; i < size; i++) {
            sqlAggregateExpr.addArgument(((SQLExpr) ((SqlValue)args.get(i)).toParseTree()));
        }
        return sqlAggregateExpr;

    }
}