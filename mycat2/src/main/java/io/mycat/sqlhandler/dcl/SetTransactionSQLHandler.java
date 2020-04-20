package io.mycat.sqlhandler.dcl;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.meta.MetadataService;
import io.mycat.proxy.session.MycatSession;
import io.mycat.util.Receiver;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Resource
public class SetTransactionSQLHandler extends AbstractSQLHandler<MySqlSetTransactionStatement> {
    @Resource
    private MetadataService mycatMetadataService;

    @PostConstruct
    public void init(){

    }

    @Override
    protected int onExecute(SQLRequest<MySqlSetTransactionStatement> request, Receiver response, MycatSession session) {
        //直接调用已实现好的
        request.getContext().tclStatementHandler().handleSetTransaction(request.getAst(), response);
        return CODE_200;
    }
}
