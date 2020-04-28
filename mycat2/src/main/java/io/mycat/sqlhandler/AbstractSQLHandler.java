package io.mycat.sqlhandler;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import io.mycat.proxy.session.MycatSession;
import io.mycat.util.ClassUtil;
import io.mycat.util.Receiver;

public abstract class AbstractSQLHandler<Statement extends SQLStatement> implements SQLHandler<Statement> {
    private final Class statementClass;
    public AbstractSQLHandler() {
        this.statementClass = ClassUtil.findGenericType(this, AbstractSQLHandler.class, "Statement");
    }
    public AbstractSQLHandler(Class statementClass) {
        this.statementClass = statementClass;
    }

    @Override
    public int execute(SQLRequest<Statement> request, Receiver response, MycatSession session) {
        if(!statementClass.isInstance(request.getAst())){
            return CODE_0;
        }
        try {
            onExecuteBefore(request,response,session);
            return onExecute(request,response,session);
        } finally {
            onExecuteAfter(request,response,session);
        }
    }

    protected void onExecuteBefore(SQLRequest<Statement> request, Receiver response, MycatSession session){}
    protected abstract int onExecute(SQLRequest<Statement> request, Receiver response, MycatSession session);
    protected void onExecuteAfter(SQLRequest<Statement> request, Receiver response, MycatSession session){

    }


}
