package io.mycat.sqlhandler;

import com.alibaba.druid.sql.ast.SQLStatement;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.Response;

/**
 * 数据库代理的执行器, 调用后端数据库接口.
 * 承接前端数据,代理数据,后端数据.
 *
 * @param <Statement> 已经分析优化或重写后的SQL语法树
 * @author wangzihaogithub 2020年4月18日23:09:18
 */
public interface SQLHandler<Statement extends SQLStatement> {
    default void execute(SQLRequest<Statement> request, MycatDataContext dataContext, Response response)throws Exception {
        response.sendError(new MycatException(request.getAst()+" not Implemented"));
    }
    public Class getStatementClass();
}
