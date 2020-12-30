package io.mycat.sqlhandler;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.Response;
import io.mycat.util.ClassUtil;
import lombok.EqualsAndHashCode;

import java.util.Objects;

@EqualsAndHashCode
public abstract class AbstractSQLHandler<Statement extends SQLStatement> implements SQLHandler<Statement> {
    private final Class statementClass;

    public AbstractSQLHandler() {
        Class<?> statement = ClassUtil.findGenericType(this, AbstractSQLHandler.class, "Statement");
        Objects.requireNonNull(statement);
        statementClass = statement;
    }

    public AbstractSQLHandler(Class statementClass) {
        this.statementClass = statementClass;
    }

    @Override
    public void execute(SQLRequest<Statement> request, MycatDataContext dataContext, Response response) throws Exception {
        try {
            onExecuteBefore(request, dataContext, response);
            onExecute(request, dataContext, response);
        } finally {
            onExecuteAfter(request, dataContext, response);
        }
    }

    protected void onExecuteBefore(SQLRequest<Statement> request, MycatDataContext dataContext, Response respons) {
    }

    protected abstract void onExecute(SQLRequest<Statement> request, MycatDataContext dataContext, Response response) throws Exception;

    protected void onExecuteAfter(SQLRequest<Statement> request, MycatDataContext dataContext, Response response) throws Exception {


    }

    public Class getStatementClass() {
        return statementClass;
    }

    public void resolveSQLExprTableSource( SQLExprTableSource tableSource,MycatDataContext dataContext) {
        if (tableSource.getSchema() == null) {
            String defaultSchema = dataContext.getDefaultSchema();
            if (defaultSchema == null) {
                throw new MycatException("please use schema");
            }
            tableSource.setSchema(defaultSchema);
        }
    }
}
