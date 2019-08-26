package cn.lightfish.sql.ast.expr;

import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.CheckColumnAmbiguous;
import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.ResolveAllColumn;
import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.ResolveIdentifierAlias;

import cn.lightfish.sql.context.GlobalContext;
import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLAlterStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLDDLStatement;
import com.alibaba.fastsql.sql.parser.SQLParserFeature;
import com.alibaba.fastsql.sql.parser.SQLParserUtils;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;
import java.util.Iterator;
import java.util.List;

public enum SQLParser {
  INSTANCE;
  public Iterator<SQLStatement> parse(String sql) {
    SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql,
        SQLParserFeature.EnableSQLBinaryOpExprGroup,
        SQLParserFeature.UseInsertColumnsCache,
        SQLParserFeature.OptimizedForParameterized);
    List<SQLStatement> sqlStatements = parser.parseStatementList();
    for (SQLStatement sqlStatement : sqlStatements) {
      if (sqlStatement instanceof SQLAlterStatement || sqlStatement instanceof SQLDDLStatement) {
        GlobalContext.INSTANCE.CACHE_REPOSITORY.accept(sqlStatement);
      }
      GlobalContext.INSTANCE.CACHE_REPOSITORY.resolve(sqlStatement, ResolveAllColumn,
          ResolveIdentifierAlias,
          CheckColumnAmbiguous);
    }

    return sqlStatements.iterator();
  }
}