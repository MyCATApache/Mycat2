package io.mycat.util;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.support.calcite.CalciteMySqlNodeVisitor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

public class MycatSqlUtil {
    public static String getCalciteSQL(SQLStatement sqlStatement) {
        SQLSelectQueryBlock queryBlock = ((SQLSelectStatement) sqlStatement).getSelect().getQueryBlock();
        CalciteMySqlNodeVisitor calciteMySqlNodeVisitor = new CalciteMySqlNodeVisitor();
        sqlStatement.accept(calciteMySqlNodeVisitor);
        SqlNode sqlNode = calciteMySqlNodeVisitor.getSqlNode();
        return sqlNode.toSqlString(MysqlSqlDialect.DEFAULT).getSql();
    }
}