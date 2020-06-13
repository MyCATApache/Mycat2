package io.mycat.sqlHandler.dcl;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import io.mycat.DefaultCommandHandler;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.upondb.MycatDBs;
import io.mycat.util.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SetTransactionSQLHandler extends AbstractSQLHandler<MySqlSetTransactionStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SetTransactionSQLHandler.class);
    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlSetTransactionStatement> request, MycatDataContext dataContext, Response response) {
        MySqlSetTransactionStatement statement = request.getAst();
        String isolationLevel = statement.getIsolationLevel();
        MySQLIsolation mySQLIsolation = MySQLIsolation.parse(isolationLevel);
        if (mySQLIsolation == null) {
            LOGGER.warn("不支持的设置值:"+statement);
            response.sendOk();
            return ExecuteCode.PERFORMED;
        }
        int jdbcValue = mySQLIsolation.getJdbcValue();
        MycatDBs.createClient(dataContext).setTransactionIsolation(jdbcValue);
        response.sendOk();
        return ExecuteCode.PERFORMED;
    }
}
