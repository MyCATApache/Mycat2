package io.mycat.sqlhandler.ddl;

import com.alibaba.fastsql.sql.ast.statement.SQLCreateTableStatement;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.meta.MetadataService;
import io.mycat.proxy.session.MycatSession;
import io.mycat.util.Receiver;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Resource
public class CreateTableSQLHandler extends AbstractSQLHandler<SQLCreateTableStatement> {
    @Resource
    private MetadataService mycatMetadataService;

    @PostConstruct
    public void init(){

    }

    @Override
    protected int onExecute(SQLRequest<SQLCreateTableStatement> request, Receiver response, MycatSession session) {
        //直接调用已实现好的
        request.getContext().ddlStatementHandler().handleCreateTable(request.getAst(), response);
        return CODE_200;
    }
}
