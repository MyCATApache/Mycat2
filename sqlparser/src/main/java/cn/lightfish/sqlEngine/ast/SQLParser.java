package cn.lightfish.sqlEngine.ast;

import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.CheckColumnAmbiguous;
import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.ResolveAllColumn;
import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.ResolveIdentifierAlias;

import cn.lightfish.sqlEngine.ast.extractor.Extractors;
import cn.lightfish.sqlEngine.context.GlobalContext;
import cn.lightfish.sqlEngine.schema.DbTable;
import cn.lightfish.sqlEngine.schema.StatementType;
import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLAlterStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLDDLStatement;
import com.alibaba.fastsql.sql.parser.SQLParserFeature;
import com.alibaba.fastsql.sql.parser.SQLParserUtils;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;

import java.util.List;
import java.util.Set;

public enum SQLParser {
  INSTANCE;
  public List<SQLStatement> parse(String sql) {
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

    return sqlStatements;
  }

  public static void main(String[] args) {
    List<SQLStatement> statements = SQLParser.INSTANCE.parse("WITH person_tom AS\n" +
            "(\n" +
            "SELECT * FROM `T_Person`\n" +
            "WHERE FName='TOM'\n" +
            ")\n" +
            "SELECT * FROM  `T_Person`\n" +
            "WHERE FAge=person_tom.FAge\n" +
            "OR FSalary=person_tom.FSalary");
    for (SQLStatement statement : statements) {
      //判断statement类型
      StatementType statementType = Extractors.getStatementType(statement);
      switch (statementType){
        case SQLSelectStatement:{
          Set<DbTable> tables = Extractors.getTables(defaultSchema.getSchemaName(), statement);
          if (tables==null||tables.isEmpty()){

          }
          System.out.println();
        }
        case SQLInsertStatement:
        case MySqlInsertStatement:
        case MySqlUpdateStatement:
        case SQLDeleteStatement:
        case MySqlDeleteStatement:

        default:
      }
      System.out.println(statementType);
    }

  }
}