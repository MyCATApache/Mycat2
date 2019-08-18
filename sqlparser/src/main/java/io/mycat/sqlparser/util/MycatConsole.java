package io.mycat.sqlparser.util;

import static com.alibaba.druid.sql.repository.SchemaResolveVisitor.Option.ResolveAllColumn;
import static com.alibaba.druid.sql.repository.SchemaResolveVisitor.Option.ResolveIdentifierAlias;

import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLCommitStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateSequenceStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropSequenceStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExplainStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.druid.sql.ast.statement.SQLRollbackStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.druid.sql.ast.statement.SQLUseStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.repository.SchemaObject;
import com.alibaba.druid.sql.visitor.SQLEvalVisitor;
import com.alibaba.druid.sql.visitor.SQLEvalVisitorUtils;
import com.alibaba.fastsql.sql.ast.statement.SQLShowStatement;
import com.sun.xml.internal.ws.server.UnsupportedMediaException;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatUpdateResponseImpl;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

public class MycatConsole {

  final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(MycatConsole.class);
  final MycatSchemaRespository schemaRespository = new MycatSchemaRespository();

  public Iterator<MycatResponse> input(String sql) {
    SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, "mysql");
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
        schemaRespository.resolve(statement, ResolveAllColumn, ResolveIdentifierAlias);
        if (statement instanceof SQLSelectStatement) {

          System.out.println();
        } else if (statement instanceof MySqlInsertStatement) {
          complie((MySqlInsertStatement) statement);
        } else if (statement instanceof MySqlUpdateStatement) {

        } else if (statement instanceof SQLDeleteStatement) {

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

  private void complie(MySqlInsertStatement statement) {
    SQLExprTableSource tableSource = statement.getTableSource();
    SchemaObject schemaObject = tableSource.getSchemaObject();
    List<SQLExpr> columns = statement.getColumns();
    List<ValuesClause> valuesList = statement.getValuesList();
    SQLEvalVisitor evalVisitor = SQLEvalVisitorUtils.createEvalVisitor("mysql");
    for (ValuesClause valuesClause : valuesList) {
      List<SQLExpr> values = valuesClause.getValues();
      for (int i = 0; i < values.size(); i++) {
        SQLExpr sqlExpr = values.get(i);
        if (sqlExpr instanceof SQLValuableExpr) {

        } else {
          sqlExpr.accept(evalVisitor);
          Object attribute = sqlExpr.getAttribute(SQLEvalVisitor.EVAL_VALUE);
          if (attribute != null) {
            values.set(i, new SQLCharExpr(attribute.toString()));
          }
        }
        SQLExpr column = columns.get(i);
        SQLDataType sqlDataType = column.computeDataType();
        System.out.println(sqlDataType);
      }
    }
    System.out.println();
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