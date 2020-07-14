package io.mycat.sqlhandler;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import io.mycat.MycatDataContext;
import io.mycat.util.ClassUtil;
import io.mycat.util.Response;

import java.util.Objects;

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
    public ExecuteCode execute(SQLRequest<Statement> request, MycatDataContext dataContext, Response response) {
        if(!statementClass.isInstance(request.getAst())){
            return ExecuteCode.NOT_PERFORMED;
        }
        try {
            onExecuteBefore(request,dataContext,response);
            return onExecute(request,dataContext,response);
        } finally {
            onExecuteAfter(request,dataContext,response);
        }
    }

    protected void onExecuteBefore(SQLRequest<Statement> request, MycatDataContext dataContext, Response respons){}
    protected abstract ExecuteCode onExecute(SQLRequest<Statement> request, MycatDataContext dataContext, Response response);
    protected  ExecuteCode onExplain(SQLRequest<Statement> request, MycatDataContext dataContext, Response response){
        return ExecuteCode.NOT_PERFORMED;
    }
    protected void onExecuteAfter(SQLRequest<Statement> request,MycatDataContext dataContext,  Response response){



    }
    @Override
    public ExecuteCode explain(SQLRequest<Statement> request, MycatDataContext dataContext, Response response) {
        if(!statementClass.isInstance(request.getAst())){
            return ExecuteCode.NOT_PERFORMED;
        }
        try {
            return onExplain(request,dataContext,response);
        } finally {

        }
    }

}
