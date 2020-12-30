package io.mycat.mpp;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLObject;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

@AllArgsConstructor
@EqualsAndHashCode
public class BooleanValue implements ASTExp {
    final boolean value;

    @Override
    public String toString() {
        return Boolean.toString(value);
    }

    @Override
    public SQLObject toParseTree() {
        return SQLUtils.toSQLExpr(toString());
    }
}