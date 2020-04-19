package io.mycat.sqlhandler.dcl;

import com.alibaba.fastsql.sql.ast.statement.SQLStartTransactionStatement;
import io.mycat.meta.MetadataService;
import io.mycat.proxy.session.MycatSession;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.util.Receiver;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Resource
public class StartTransactionSQLHandler extends AbstractSQLHandler<SQLStartTransactionStatement> {
    @Resource
    private MetadataService mycatMetadataService;

    @PostConstruct
    public void init(){

    }

    @Override
    protected int onExecute(SQLRequest<SQLStartTransactionStatement> request, Receiver response, MycatSession session) {
        //直接调用已实现好的
        request.getContext().tclStatementHandler().handleSQLStartTransaction(request.getAst(), response);
        return CODE_200;
    }
}
