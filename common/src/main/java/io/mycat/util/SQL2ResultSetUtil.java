package io.mycat.util;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.util.MycatRowMetaDataImpl;

import java.util.List;

public class SQL2ResultSetUtil {

    public static MycatRowMetaDataImpl getMycatRowMetaData(String createTableStmtText) {
        List<SQLStatement> statements = SQLUtils.parseStatements(createTableStmtText, DbType.mysql);
        MySqlCreateTableStatement mySqlCreateTableStatement = (MySqlCreateTableStatement) statements.get(statements.size() - 1);
        String tableName = mySqlCreateTableStatement.getTableSource().computeAlias();
        return new MycatRowMetaDataImpl(mySqlCreateTableStatement.getColumnDefinitions(), "", tableName);
    }

}