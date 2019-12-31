/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.sqlEngine.ast;

import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.CheckColumnAmbiguous;
import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.ResolveAllColumn;
import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.ResolveIdentifierAlias;

import io.mycat.sqlEngine.ast.extractor.Extractors;
import io.mycat.sqlEngine.context.GlobalContext;
import io.mycat.sqlEngine.schema.StatementType;
import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLAlterStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLDDLStatement;
import com.alibaba.fastsql.sql.parser.SQLParserFeature;
import com.alibaba.fastsql.sql.parser.SQLParserUtils;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;

import java.util.List;

/**
 * @author Junwen Chen
 **/
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
//          Set<DbTable> tables = Extractors.getTables(defaultSchema.getSchemaName(), statement);
//          if (tables==null||tables.isEmpty()){
//
//          }
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