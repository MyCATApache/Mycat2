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
package io.mycat.sqlparser;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.util.JdbcConstants;
import io.mycat.sqlparser.visitor.MyStatementVisitor;
import java.util.List;
import net.sf.jsqlparser.JSQLParserException;

/**
 * @author jamie12221
 * @date 2019-05-06 12:33
 **/
public class SQLParser {
  public static void parse(String sql) throws JSQLParserException {
    String dbType = JdbcConstants.MYSQL;
    List<SQLStatement> statementList = SQLUtils.parseStatements(sql, dbType);
    MySqlASTVisitor myStatementVisitor = new MyStatementVisitor();

    for (SQLStatement statement : statementList) {

      if(statement instanceof SQLSelectStatement){
        SQLSelect select = ((SQLSelectStatement) statement).getSelect();
        SQLSelectQueryBlock firstQueryBlock = select.getFirstQueryBlock();
        SchemaStatVisitor schemaStatVisitor = new SchemaStatVisitor();
        schemaStatVisitor.visit(firstQueryBlock);

        System.out.println();

      }


    }


  }

  public static void main(String[] args) throws JSQLParserException {
    SQLParser.parse("select id from travelrecord;select id from travelrecord;");
  }

}
