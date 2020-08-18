package io.mycat.sqlhandler.dml;

import com.alibaba.fastsql.sql.ast.statement.SQLAssignItem;
import com.alibaba.fastsql.sql.ast.statement.SQLSetStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;

import java.util.Collections;
import java.util.List;
import java.util.Objects;


public class SetSQLHandler extends AbstractSQLHandler<SQLSetStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLSetStatement> request, MycatDataContext dataContext, Response response) {
        List<SQLAssignItem> items = request.getAst().getItems();
        if (items == null) {
            items = Collections.emptyList();
        }
        for (SQLAssignItem item : items) {
            String name = Objects.toString(item.getTarget()).toLowerCase();
            String value = Objects.toString(item.getValue());
            dataContext.setVariable(name, value);
        }
        response.sendOk();
        return ExecuteCode.PERFORMED;
    }

    public SetSQLHandler() {
    }

    public SetSQLHandler(Class statementClass) {
        super(statementClass);
    }
}