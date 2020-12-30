package io.mycat.mpp;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;

import java.util.HashMap;

public class RegExpNode extends BooleanBinaryOp {

    protected RegExpNode(SqlValue left, SqlValue right, Invoker fun) {
        super(SQLBinaryOperator.RegExp, left, right, fun);
    }

    public static RegExpNode create(SqlValue left, SqlValue right) {
        Invoker fun = Ops.resolveReturnBoolean(hashMap, left.getType(), right.getType());
        return new RegExpNode(left, right, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, RegExpNode.class);
    }

}