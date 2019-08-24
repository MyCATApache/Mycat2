package io.mycat.schema;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatUpdateResponseImpl;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.sqlparser.MycatConsoleResult;
import io.mycat.sqlparser.MycatConsoleResultImpl;
import io.mycat.sqlparser.MycatParser;
import io.mycat.sqlparser.ResultOk;
import io.mycat.sqlparser.StatementDispatcher;
import io.mycat.sqlparser.util.dataLayout.InsertDataAffinity;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MycatConsole {
  final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(MycatConsole.class);
  MycatSchema currentSchema;


  public Iterator<MycatConsoleResult> input(String sql) {
    Iterator<SQLStatement> statementIterator = MycatParser.INSTANCE.parse(sql);
    return new Iterator<MycatConsoleResult>() {
      @Override
      public boolean hasNext() {
        return statementIterator.hasNext();
      }

      @Override
      public MycatConsoleResult next() {
        SQLStatement statement = statementIterator.next();
        StatementDispatcher statementDispatcher = new StatementDispatcher(MycatConsole.this);
        statement.accept(statementDispatcher);
        return response(statementDispatcher);
      }

      private MycatConsoleResult response(StatementDispatcher mycatStatementVisitor) {
        return mycatStatementVisitor.getConsoleResult();
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
//    SQLExprTableSource tableSource = statement.getTableSource();
//    SchemaObject tableSchema = tableSource.getSchemaObject();
//    List<SQLExpr> columns = statement.getColumns();
//    InsertDataAffinity dataAffinity = dataLayoutRespository
//        .getInsertTableDataLayout(tableSource, columns);
//    List<ValuesClause> valuesList = statement.getValuesList();
//    for (ValuesClause valuesClause : valuesList) {
//      List<SQLExpr> values = valuesClause.getValues();
//      for (int i = 0; i < values.size(); i++) {
//        SQLExpr sqlExpr = values.get(i);
//        sqlExpr = constantFolding(sqlExpr);
//        values.set(i, sqlExpr);
//      }
//      dataAffinity.insert(values);
//    }
//    dataAffinity.saveParseTree(statement);
//    return dataAffinity;
    return null;
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
    PrintStream out = System.out;
    MycatConsole console = MycatSchemaManager.INSTANCE.createConsole();
    String text = new String(Files.readAllBytes(
        Paths.get(MycatConsole.class.getClassLoader().getResource("test.txt").toURI())
            .toAbsolutePath()));
    Iterator<MycatConsoleResult> iterator = console.input(text);
    int id = 1;
    while (iterator.hasNext()) {
      MycatConsoleResult result = iterator.next();
      if (result == null) {
        continue;
      }

      out.println("-------------------------------------------------------------------------");
      out.println("id:" + id);
      ++id;
      if (result instanceof ResultOk) {
        out.println("ok");
        continue;
      }
      String columnText = String.join("|", result.columnDefList());

      char[] w = new char[columnText.length()];
      Arrays.fill(w, '-');

      out.println(new String(w));
      out.println(columnText);
      out.println(new String(w));
      Iterator<Object[]> rowIterator = result.rowIterator();
      while (rowIterator.hasNext()) {
        Object[] rowList = rowIterator.next();
        String rowText = Arrays.asList(rowList).stream().map(i -> i.toString())
            .collect(Collectors.joining("|", "|", "|"));
        out.println(rowText);
      }
    }
    System.out.println();
  }

  public MycatConsoleResult createSchema(String databaseName) {
    MycatSchema mycatSchema = MycatSchemaManager.INSTANCE.schemas.computeIfAbsent(databaseName, MycatSchema::new);
    if (currentSchema == null) {
      currentSchema = mycatSchema;
    }
    return new ResultOk();
  }

  public MycatConsoleResult showDatabase() {
    MycatConsoleResultImpl mycatConsoleResult = new MycatConsoleResultImpl(1);
    mycatConsoleResult.addColumn("Database");
    for (String database : MycatSchemaManager.INSTANCE.schemas.keySet()) {
      mycatConsoleResult.addRow(database);
    }

    return mycatConsoleResult;
  }

  public MycatConsoleResult createTable(MycatTable table) {
    currentSchema.createTable(table);
    return new ResultOk();
  }

  public MycatConsoleResult dropDatabase(String databaseName) {
    MycatSchemaManager.INSTANCE.schemas.remove(databaseName);
    return new ResultOk();
  }

  public MycatConsoleResult dropTable(String tableGroupName) {
    currentSchema.dropTable(tableGroupName);
    return new ResultOk();
  }

  public MycatConsoleResult dropTable(List<String> nameList) {
    currentSchema.dropTable(nameList);
    return new ResultOk();
  }

  public MycatSchema getCurrnetSchema() {
    return currentSchema;
  }

  public void delete(MycatTable table, int dataNodeIndex, MySqlInsertStatement x) {

  }

  public void insert(MycatTable table, Map<Integer, List<ValuesClause>> dataNodeIndex,
      MySqlInsertStatement x) {

  }
}