package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.meta.MetadataService;
import io.mycat.proxy.session.MycatSession;
import io.mycat.util.Receiver;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Resource
public class SelectSQLHandler extends AbstractSQLHandler<SQLSelectStatement> {
    @Resource
    private MetadataService mycatMetadataService;

    @PostConstruct
    public void init(){

    }

    @Override
    protected int onExecute(SQLRequest<SQLSelectStatement> request, Receiver response, MycatSession session) {
        //直接调用已实现好的
        request.getContext().selectStatementHandler().handleSelect(request.getAst(),response);
        return CODE_200;
    }
}
