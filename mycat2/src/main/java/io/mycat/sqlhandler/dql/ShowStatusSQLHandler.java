package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowStatusStatement;
import io.mycat.meta.MetadataService;
import io.mycat.proxy.session.MycatSession;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.util.Receiver;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Resource
public class ShowStatusSQLHandler extends AbstractSQLHandler<MySqlShowStatusStatement> {
    @Resource
    private MetadataService mycatMetadataService;

    @PostConstruct
    public void init(){

    }

    @Override
    protected int onExecute(SQLRequest<MySqlShowStatusStatement> request, Receiver response, MycatSession session) {
        //直接调用已实现好的
        response.proxyShow(request.getAst());
        return CODE_200;
    }
}
