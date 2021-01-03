package io.mycat.sqlhandler.dcl;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import io.mycat.MycatDataContext;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SetTransactionSQLHandler extends AbstractSQLHandler<MySqlSetTransactionStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SetTransactionSQLHandler.class);

    @Override
    protected void onExecute(SQLRequest<MySqlSetTransactionStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        MySqlSetTransactionStatement statement = request.getAst();
        String isolationLevel = statement.getIsolationLevel();
        MySQLIsolation mySQLIsolation = MySQLIsolation.parse(isolationLevel);
        if (mySQLIsolation == null) {
            LOGGER.warn("不支持的设置值:" + statement);
            response.sendOk();
            return;
        }
        int jdbcValue = mySQLIsolation.getJdbcValue();
        dataContext.setIsolation(MySQLIsolation.parseJdbcValue(jdbcValue));
        response.sendOk();
        return;
    }
}
