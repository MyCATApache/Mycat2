package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.meta.MetadataService;
import io.mycat.proxy.session.MycatSession;
import io.mycat.util.Receiver;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Resource
public class HintSQLHandler extends AbstractSQLHandler<MySqlHintStatement> {
    @Resource
    private MetadataService mycatMetadataService;

    @PostConstruct
    public void init(){

    }

    @Override
    protected int onExecute(SQLRequest<MySqlHintStatement> request, Receiver response, MycatSession session) {
        //直接调用已实现好的
        request.getContext().hintStatementHanlder().handlehintStatement(request.getAst(),response);
        return CODE_200;
    }
}
