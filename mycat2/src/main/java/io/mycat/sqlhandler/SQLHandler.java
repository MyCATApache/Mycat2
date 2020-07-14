package io.mycat.sqlhandler;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.util.Response;

/**
 * 数据库代理的执行器, 调用后端数据库接口.
 * 承接前端数据,代理数据,后端数据.
 *
 * @param <Statement> 已经分析优化或重写后的SQL语法树
 * @author wangzihaogithub 2020年4月18日23:09:18
 */
public interface SQLHandler<Statement extends SQLStatement> {
    int CODE_0 = 0;//mycat状态码-未执行
    int CODE_100 = 100;//mycat状态码-未执行完,等待下次请求继续执行
    int CODE_200 = 200;//mycat状态码-执行正常
    int CODE_300 = 300;//mycat状态码-代理错误
    int CODE_400 = 400;//mycat状态码-客户端错误
    int CODE_500 = 500;//mycat状态码-服务端错误

    /**
     * 根据SQL语法树执行具体逻辑
     *
     * @param request  已经分析优化或重写后的SQL语法树
     * @param response 前端用户链接
     * @return mycat状态码
     */
    default ExecuteCode execute(SQLRequest<Statement> request, MycatDataContext dataContext, Response response){
        response.sendError(new MycatException(request.getAst()+" not Implemented"));
        return ExecuteCode.PERFORMED;
    }

    default ExecuteCode explain(SQLRequest<Statement> request, MycatDataContext dataContext, Response response) {
       return ExecuteCode.NOT_PERFORMED;
    }


}
