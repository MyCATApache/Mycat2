package io.mycat.sqlhandler.dml;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.meta.MetadataService;
import io.mycat.proxy.session.MycatSession;
import io.mycat.util.Receiver;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Resource
public class UpdateSQLHandler extends AbstractSQLHandler<MySqlUpdateStatement> {
    @Resource
    private MetadataService mycatMetadataService;

    @PostConstruct
    public void init(){

    }

    @Override
    protected int onExecute(SQLRequest<MySqlUpdateStatement> request, Receiver response, MycatSession session) {
        //直接调用已实现好的
        request.getContext().updateStatementHandler().handleUpdate(request.getAst(), response);
        return CODE_200;
    }
}
