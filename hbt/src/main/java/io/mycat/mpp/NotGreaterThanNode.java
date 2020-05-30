package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;

import java.util.HashMap;

public class NotGreaterThanNode  extends BooleanBinaryOp {

    protected NotGreaterThanNode(SqlValue left, SqlValue right, Invoker fun) {
        super(SQLBinaryOperator.NotGreaterThan, left, right, fun);
    }

    public static NotGreaterThanNode create(SqlValue left, SqlValue right) {
        Invoker fun = Ops.resolveReturnBoolean(hashMap, left.getType(), right.getType());
        return new NotGreaterThanNode(left, right, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, NotGreaterThanNode.class);
    }

}