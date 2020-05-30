package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.expr.SQLExprUtils;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.RowType;
import io.mycat.mpp.runtime.Type;

import java.math.BigInteger;

public class BigIntSqlValue implements SqlValue{
    final BigInteger value;

    public BigIntSqlValue(BigInteger value) {
        this.value = value;
    }
    public static BigIntSqlValue create(BigInteger value) {
       return new BigIntSqlValue(value);
    }
    @Override
    public Object getValue(RowType type, DataAccessor dataAccessor, DataContext context) {
        return value;
    }

    @Override
    public Type getType() {
        return Type.of(Type.INT,false);
    }

    @Override
    public SQLObject toParseTree() {
        return SQLExprUtils.fromJavaObject(value);
    }
}