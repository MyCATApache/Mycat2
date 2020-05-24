package io.mycat.mpp;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLObject;
import com.google.protobuf.Message;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.Type;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.text.MessageFormat;

@AllArgsConstructor
@Builder
public class BinaryOp implements SqlValue {
    ASTExp left;
    ASTExp right;
    String op;

    @Override
    public String toString() {
        return MessageFormat.format("%s %s %s", left, op, right);
    }

    @Override
    public SQLObject toParseTree() {
        return SQLUtils.toSQLExpr(toString());
    }



    @Override
    public Object getValue(Type type, DataAccessor dataAccessor, DataContext context) {
        return null;
    }

    @Override
    public boolean getValueAsBoolean(Type columns, DataAccessor dataAccessor, DataContext dataContext) {
        return false;
    }
}
