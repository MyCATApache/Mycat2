package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;

import java.util.HashMap;

public class NotRegExpNode extends BooleanBinaryOp {

    protected NotRegExpNode(SqlValue left, SqlValue right, Invoker fun) {
        super(SQLBinaryOperator.NotRegExp, left, right, fun);
    }

    public static NotRegExpNode create(SqlValue left, SqlValue right) {
        Invoker fun = Ops.resolveReturnBoolean(hashMap, left.getType(), right.getType());
        return new NotRegExpNode(left, right, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, NotRegExpNode.class);
    }

}