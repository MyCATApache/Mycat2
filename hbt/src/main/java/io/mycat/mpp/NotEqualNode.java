package io.mycat.mpp;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;

import java.util.HashMap;

public class NotEqualNode extends BooleanBinaryOp {

    protected NotEqualNode(SqlValue left, SqlValue right, Invoker fun) {
        super(SQLBinaryOperator.NotLessThan, left, right, fun);
    }

    public static NotEqualNode create(SqlValue left, SqlValue right) {
        Invoker fun = Ops.resolveReturnBoolean(hashMap, left.getType(), right.getType());
        return new NotEqualNode(left, right, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, NotEqualNode.class);
    }

}