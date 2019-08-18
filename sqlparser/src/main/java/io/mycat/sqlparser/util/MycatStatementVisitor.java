package io.mycat.sqlparser.util;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.repository.SchemaRepository;
import java.util.List;

public class MycatStatementVisitor extends MySqlASTVisitorAdapter {

  @Override
  public boolean visit(SQLSelectItem x) {
    return super.visit(x);
  }

  @Override
  public boolean visit(MySqlSelectQueryBlock x) {
//    List<SQLSelectItem> selectList = x.getSelectList();
//    SQLTableSource from = x.getFrom();
//    SQLExpr where = x.getWhere();
//    SQLSelectGroupByClause groupBy = x.getGroupBy();
//    SQLOrderBy orderBy = x.getOrderBy();
//    SQLLimit limit = x.getLimit();
    List<SQLSelectItem> selectList = x.getSelectList();
    SQLTableSource from = x.getFrom();
    SQLExpr where = x.getWhere();
    SQLSelectGroupByClause groupBy = x.getGroupBy();
    SQLOrderBy orderBy = x.getOrderBy();
    SQLLimit limit = x.getLimit();
    return super.visit(x);
  }

  @Override
  public void endVisit(MySqlSelectQueryBlock x) {
    super.endVisit(x);
  }

  @Override
  public boolean visit(SQLIdentifierExpr x) {
    return super.visit(x);
  }

  @Override
  public boolean visit(SQLExprTableSource x) {
    return super.visit(x);

  }

  public static void main(String[] args) {
    SQLStatementParser mysql = SQLParserUtils
        .createSQLStatementParser(
            "/* mycat:schema = test2 */ select (select 1),2,c from d.travelrecord,d.c where c.id =1 or d=1 for update",
            "mysql");
    List<SQLStatement> sqlStatements = mysql.parseStatementList();
    System.out.println(sqlStatements);
    SchemaRepository schemaRepository = new SchemaRepository("mysql");
    schemaRepository.acceptDDL("CREATE TABLE `travelrecord` (\n"
        + "  `id` bigint(20) NOT NULL,\n"
        + "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n"
        + "  `traveldate` date DEFAULT NULL,\n"
        + "  `fee` decimal(10,0) DEFAULT NULL,\n"
        + "  `days` int(11) DEFAULT NULL,\n"
        + "  `blob` longblob DEFAULT NULL,\n"
        + "  `d` double DEFAULT NULL\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    schemaRepository.acceptDDL("SHOW CREATE TABLE travelrecord;");
    for (SQLStatement sqlStatement : sqlStatements) {
      MySqlSchemaStatVisitor tableAliasCollector = new MySqlSchemaStatVisitor();
      sqlStatement.accept(tableAliasCollector);
//      System.out.println(tableAliasCollector.getTableSources());
      System.out.println();
    }
  }
}