package io.mycat.sqlHandler.dcl;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.upondb.MycatDBs;
import io.mycat.util.Response;




public class SetTransactionSQLHandler extends AbstractSQLHandler<MySqlSetTransactionStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlSetTransactionStatement> request, MycatDataContext dataContext, Response response) {
        MySqlSetTransactionStatement statement = request.getAst();
        String isolationLevel = statement.getIsolationLevel();
        MySQLIsolation mySQLIsolation = MySQLIsolation.parse(isolationLevel);
        if (mySQLIsolation == null) {
            response.sendError(new MycatException("非法字符串:" + isolationLevel));
            return ExecuteCode.PERFORMED;
        }
        int jdbcValue = mySQLIsolation.getJdbcValue();
        MycatDBs.createClient(dataContext).setTransactionIsolation(jdbcValue);
        response.sendOk();
        return ExecuteCode.PERFORMED;
    }
}
