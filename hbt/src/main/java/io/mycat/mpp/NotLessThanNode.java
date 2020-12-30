package io.mycat.mpp;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;

import java.util.HashMap;

public class NotLessThanNode extends BooleanBinaryOp {

    protected NotLessThanNode(SqlValue left, SqlValue right, Invoker fun) {
        super(SQLBinaryOperator.NotLessThan, left, right, fun);
    }

    public static NotLessThanNode create(SqlValue left, SqlValue right) {
        Invoker fun = Ops.resolveReturnBoolean(hashMap, left.getType(), right.getType());
        return new NotLessThanNode(left, right, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, NotLessThanNode.class);
    }

}