package io.mycat.sqlhandler.ddl;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.meta.MetadataService;
import io.mycat.proxy.session.MycatSession;
import io.mycat.util.Receiver;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Resource
public class RenameTableSQLHandler extends AbstractSQLHandler<MySqlRenameTableStatement> {
    @Resource
    private MetadataService mycatMetadataService;

    @PostConstruct
    public void init(){

    }

    @Override
    protected int onExecute(SQLRequest<MySqlRenameTableStatement> request, Receiver response, MycatSession session) {
        //直接调用已实现好的
        request.getContext().ddlStatementHandler().handleMySqlRenameTable(request.getAst(), response);
        return CODE_200;
    }
}
