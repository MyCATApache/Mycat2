package io.mycat.sqlHandler.dml;

import com.alibaba.fastsql.sql.ast.statement.SQLAssignItem;
import com.alibaba.fastsql.sql.ast.statement.SQLSetStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.upondb.MycatDBs;
import io.mycat.util.Response;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Resource
public class SetSQLHandler extends AbstractSQLHandler<SQLSetStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLSetStatement> request, MycatDataContext dataContext, Response response) {
        List<SQLAssignItem> items = request.getAst().getItems();
        if (items == null) {
            items = Collections.emptyList();
        }
        MycatDBClientMediator client = MycatDBs.createClient(dataContext);
        for (SQLAssignItem item : items) {
            String name = Objects.toString(item.getTarget()).toLowerCase();
            String value = Objects.toString(item.getValue());
            client.setVariable(name, value);
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