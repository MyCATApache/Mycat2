package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.SQLObject;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class NullValue implements ASTExp {
    @Override
    public String toString() {
        return "NULL";
    }

    @Override
    public SQLObject toParseTree() {
        return null;
    }
}