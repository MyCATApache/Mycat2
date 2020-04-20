package io.mycat.sqlhandler.ddl;

import com.alibaba.fastsql.sql.ast.statement.SQLDropViewStatement;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.meta.MetadataService;
import io.mycat.proxy.session.MycatSession;
import io.mycat.util.Receiver;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Resource
public class DropViewSQLHandler extends AbstractSQLHandler<SQLDropViewStatement> {
    @Resource
    private MetadataService mycatMetadataService;

    @PostConstruct
    public void init(){

    }

    @Override
    protected int onExecute(SQLRequest<SQLDropViewStatement> request, Receiver response, MycatSession session) {
        //直接调用已实现好的
        request.getContext().ddlStatementHandler().handleDropViewStatement(request.getAst(), response);
        return CODE_200;
    }
}
