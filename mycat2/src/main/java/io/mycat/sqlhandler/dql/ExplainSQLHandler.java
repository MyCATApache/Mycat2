package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.meta.MetadataService;
import io.mycat.proxy.session.MycatSession;
import io.mycat.util.Receiver;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Resource
public class ExplainSQLHandler extends AbstractSQLHandler<MySqlExplainStatement> {
    @Resource
    private MetadataService mycatMetadataService;

    @PostConstruct
    public void init(){

    }

    @Override
    protected int onExecute(SQLRequest<MySqlExplainStatement> request, Receiver response, MycatSession session) {
        //直接调用已实现好的
        request.getContext().utilityStatementHandler().handleExplain(request.getAst(), response);
        return CODE_200;
    }
}
