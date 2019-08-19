package io.mycat.sqlparser.util;

import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.ResolveAllColumn;
import static com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option.ResolveIdentifierAlias;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOpExprGroup;
import com.alibaba.fastsql.sql.ast.expr.SQLCharExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLValuableExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLAlterStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLAlterViewStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLCommitStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLCreateSequenceStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLDDLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLDropDatabaseStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLDropSequenceStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExplainStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.fastsql.sql.ast.statement.SQLRollbackStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelect;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSetStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLShowStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLUseStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.fastsql.sql.optimizer.Optimizers;
import com.alibaba.fastsql.sql.parser.SQLParserFeature;
import com.alibaba.fastsql.sql.parser.SQLParserUtils;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;
import com.alibaba.fastsql.sql.repository.SchemaObject;
import com.alibaba.fastsql.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.fastsql.sql.visitor.SQLEvalVisitor;
import com.alibaba.fastsql.sql.visitor.SQLEvalVisitorUtils;
import com.sun.xml.internal.ws.server.UnsupportedMediaException;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatUpdateResponseImpl;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.sqlparser.util.complie.ConstantFoldingRepository;
import io.mycat.sqlparser.util.complie.RangeLayoutResolver;
import io.mycat.sqlparser.util.dataLayout.InsertDataAffinity;
import io.mycat.sqlparser.util.dataLayout.DataLayoutRespository;
import io.mycat.sqlparser.util.dataLayout.MySQLDataAffinityLayout;
import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.cnfexpression.CNFConverter;

public class MycatConsole {

  final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(MycatConsole.class);
  final MycatSchemaRespository schemaRespository = new MycatSchemaRespository();
  final DataLayoutRespository dataLayoutRespository = new DataLayoutRespository();
  final ConstantFoldingRepository constantFoldingRepository = new ConstantFoldingRepository();

  public Iterator<MycatResponse> input(String sql) {
    SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, "mysql",
        SQLParserFeature.EnableSQLBinaryOpExprGroup,
        SQLParserFeature.UseInsertColumnsCache,
        SQLParserFeature.OptimizedForParameterized);
    List<SQLStatement> sqlStatements = parser.parseStatementList();
    Iterator<SQLStatement> iterator = sqlStatements.iterator();
    return new Iterator<MycatResponse>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public MycatResponse next() {
        SQLStatement statement = iterator.next();
        System.out.println("----------------before--------------------");
        System.out.println(statement);
        Optimizers.optimize(statement, DbType.mysql);
        System.out.println("----------------after--------------------");
        System.out.println(statement);
        System.out.println("---------------------------------");

        if (statement instanceof SQLSelectStatement) {
          SQLSelect select = ((SQLSelectStatement) statement).getSelect();
          SQLSelectQueryBlock queryBlock = select.getQueryBlock();
          SQLTableSource from = queryBlock.getFrom();
          if (from!=null){
            SQLExpr where = queryBlock.getWhere();
            if (where!=null){
              RangeLayoutResolver rangeLayoutResolver = new RangeLayoutResolver(from,
                  schemaRespository);
              where.accept(rangeLayoutResolver);
              System.out.println();
            }
          }
          System.out.println();
        } else if (statement instanceof MySqlInsertStatement) {
          InsertDataAffinity complie = complie((MySqlInsertStatement) statement);
          MySQLDataAffinityLayout mySQLDataAffinityLayout = new MySQLDataAffinityLayout();
          Map<Integer, String> integerStringMap = mySQLDataAffinityLayout
              .insertDataAffinity(complie);
          System.out.println(integerStringMap);
        } else if (statement instanceof MySqlUpdateStatement) {
          complie((MySqlUpdateStatement) statement);
        } else if (statement instanceof SQLDeleteStatement) {
          complie((SQLDeleteStatement) statement);
        } else if (statement instanceof SQLSetStatement) {

        } else if (statement instanceof SQLCommitStatement) {

        } else if (statement instanceof SQLRollbackStatement) {

        } else if (statement instanceof SQLUseStatement) {

        } else if (statement instanceof MySqlSetTransactionStatement) {

        } else if (statement instanceof SQLAlterStatement) {
          if (statement instanceof SQLAlterTableStatement) {

          }
        } else if (statement instanceof SQLShowStatement) {

        } else if (statement instanceof SQLDDLStatement) {
          if (statement instanceof SQLCreateDatabaseStatement) {
            String databaseName = ((SQLCreateDatabaseStatement) statement).getName()
                .getSimpleName();
            schemaRespository.setDefaultSchema(databaseName);
            return responseOk();
          } else if (statement instanceof SQLDropDatabaseStatement) {
            SQLName sqlName = (SQLName) ((SQLDropDatabaseStatement) statement).getDatabase();
            String databaseName = sqlName.getSimpleName();
            schemaRespository.dropSchema(databaseName);
          } else if (statement instanceof SQLDropSequenceStatement) {
            schemaRespository.accept(statement);
          } else if (statement instanceof SQLCreateSequenceStatement) {
            schemaRespository.accept(statement);
          } else if (statement instanceof MySqlCreateTableStatement) {
            schemaRespository.accept(statement);
          } else if (statement instanceof SQLDropTableStatement) {
            schemaRespository.accept(statement);
          } else if (statement instanceof SQLCreateViewStatement) {
            schemaRespository.accept(statement);
          } else if (statement instanceof SQLAlterViewStatement) {
            schemaRespository.accept(statement);
          } else if (statement instanceof SQLCreateIndexStatement) {
            schemaRespository.accept(statement);
          } else if (statement instanceof SQLCreateFunctionStatement) {
            schemaRespository.accept(statement);
          } else if (statement instanceof SQLAlterTableStatement) {
            schemaRespository.accept(statement);
          } else if (statement instanceof SQLDropIndexStatement) {
            schemaRespository.accept(statement);
          } else {
            throw new UnsupportedMediaException();
          }
        } else if (statement instanceof SQLExplainStatement) {
          SQLStatement statement1 = ((SQLExplainStatement) statement).getStatement();
        } else {
          throw new UnsupportedMediaException();
        }
        return responseOk();
      }
    };
  }

  private void complie(SQLDeleteStatement statement) {
    SQLName tableName = statement.getTableName();
    SQLExpr where = statement.getWhere();

  }

  private InsertDataAffinity complie(MySqlUpdateStatement statement) {
    return null;
  }


  private InsertDataAffinity complie(MySqlInsertStatement statement) {
    SQLExprTableSource tableSource = statement.getTableSource();
    SchemaObject tableSchema = tableSource.getSchemaObject();
    List<SQLExpr> columns = statement.getColumns();
    InsertDataAffinity dataAffinity = dataLayoutRespository.getTableDataLayout(tableSource, columns);
    List<ValuesClause> valuesList = statement.getValuesList();
    for (ValuesClause valuesClause : valuesList) {
      List<SQLExpr> values = valuesClause.getValues();
      for (int i = 0; i < values.size(); i++) {
        SQLExpr sqlExpr = values.get(i);
        sqlExpr = constantFolding(sqlExpr);
        values.set(i, sqlExpr);
      }
      dataAffinity.insert(values);
    }
    dataAffinity.saveParseTree(statement);
    return dataAffinity;
}

  private SQLExpr constantFolding(SQLExpr sqlExpr) {
//    if (sqlExpr instanceof SQLValuableExpr) {
//
//    } else {
//      sqlExpr.accept(SQL_EVAL_VISITOR);
//      Object value = sqlExpr.getAttribute(SQLEvalVisitor.EVAL_VALUE);
//      if (value != null) {
//        sqlExpr = new SQLCharExpr(value.toString());
//      }
//    }

    return sqlExpr;
  }

  private MycatResponse responseOk() {
    return new MycatUpdateResponseImpl(0, 0, 0);
  }

  public static void main(String[] args) throws IOException, URISyntaxException {
    MycatConsole console = new MycatConsole();
    String text = new String(Files.readAllBytes(
        Paths.get(MycatConsole.class.getClassLoader().getResource("test.txt").toURI())
            .toAbsolutePath()));
    Iterator<MycatResponse> iterator = console.input(text);
    while (iterator.hasNext()) {
      MycatResponse next = iterator.next();
    }
    System.out.println();
  }
}