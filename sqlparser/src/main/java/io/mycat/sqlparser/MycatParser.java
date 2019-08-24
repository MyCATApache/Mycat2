package io.mycat.sqlparser;

import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.CheckColumnAmbiguous;
import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.ResolveAllColumn;
import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.ResolveIdentifierAlias;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLAlterStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLDDLStatement;
import com.alibaba.fastsql.sql.optimizer.Optimizers;
import com.alibaba.fastsql.sql.parser.SQLParserFeature;
import com.alibaba.fastsql.sql.parser.SQLParserUtils;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;
import com.alibaba.fastsql.sql.repository.SchemaRepository;
import java.util.Iterator;
import java.util.List;

public enum MycatParser {
  INSTANCE;
  final static SchemaRepository CACHE_REPOSITORY = new SchemaRepository(DbType.mysql);

  public Iterator<SQLStatement> parse(String sql) {
    SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql,
        SQLParserFeature.EnableSQLBinaryOpExprGroup,
        SQLParserFeature.UseInsertColumnsCache,
        SQLParserFeature.OptimizedForParameterized);
    List<SQLStatement> sqlStatements = parser.parseStatementList();
    for (SQLStatement sqlStatement : sqlStatements) {
      if (sqlStatement instanceof SQLAlterStatement||sqlStatement instanceof SQLDDLStatement){
        CACHE_REPOSITORY.accept(sqlStatement);
      }
      CACHE_REPOSITORY.resolve(sqlStatement, ResolveAllColumn,
          ResolveIdentifierAlias,
          CheckColumnAmbiguous);
      Optimizers.optimize(sqlStatement, DbType.mysql,CACHE_REPOSITORY);
    }
    return sqlStatements.iterator();
  }
}