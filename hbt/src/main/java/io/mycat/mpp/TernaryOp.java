package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.SQLObject;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class TernaryOp implements ASTExp {
    ASTExp cond;
    ASTExp first;
    ASTExp sec;
    String op;

    @Override
    public String toString() {
        return String.format("%s %s:%s:%s", op, cond, first, sec);
    }

    @Override
    public SQLObject toParseTree() {
        return null;
    }
}