package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.ast.statement.SQLShowColumnsStatement;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.meta.MetadataService;
import io.mycat.proxy.session.MycatSession;
import io.mycat.util.Receiver;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Resource
public class ShowColumnsSQLHandler extends AbstractSQLHandler<SQLShowColumnsStatement> {
    @Resource
    private MetadataService mycatMetadataService;

    @PostConstruct
    public void init(){

    }

    @Override
    protected int onExecute(SQLRequest<SQLShowColumnsStatement> request, Receiver response, MycatSession session) {
        //直接调用已实现好的
        request.getContext().showStatementHandler().handleMySqlShowColumns(request.getAst(), response);
        return CODE_200;
    }
}
