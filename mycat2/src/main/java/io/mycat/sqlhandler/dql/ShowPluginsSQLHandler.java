package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCharacterSetStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowPluginsStatement;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;

import java.sql.JDBCType;

public class ShowPluginsSQLHandler  extends AbstractSQLHandler<MySqlShowPluginsStatement> {
    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlShowPluginsStatement> request, MycatDataContext dataContext, Response response) {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("Name", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("Status", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("Type", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("Library",JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("License",JDBCType.VARCHAR);
        return response.sendResultSet(resultSetBuilder.build());
    }
}
