package io.mycat.sqlhandler.dcl;

import com.alibaba.fastsql.sql.ast.statement.SQLCommitStatement;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.meta.MetadataService;
import io.mycat.proxy.session.MycatSession;
import io.mycat.util.Receiver;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Resource
public class CommitSQLHandler extends AbstractSQLHandler<SQLCommitStatement> {
    @Resource
    private MetadataService mycatMetadataService;

    @PostConstruct
    public void init(){

    }

    @Override
    protected int onExecute(SQLRequest<SQLCommitStatement> request, Receiver response, MycatSession session) {
        //直接调用已实现好的
        request.getContext().tclStatementHandler().handleCommit(request.getAst(), response);
        return CODE_200;
    }
}
