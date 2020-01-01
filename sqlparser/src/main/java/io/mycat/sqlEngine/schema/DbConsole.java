package io.mycat.sqlEngine.schema;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.sqlEngine.ast.SQLParser;
import io.mycat.sqlEngine.ast.complier.ComplierContext;
import io.mycat.sqlEngine.ast.statement.StatementDispatcher;
import io.mycat.sqlEngine.context.RootSessionContext;
import io.mycat.sqlEngine.executor.DefExecutor;
import io.mycat.sqlEngine.executor.EmptyExecutor;
import io.mycat.sqlEngine.executor.PhysicsExecutorRunner;
import io.mycat.sqlEngine.executor.logicExecutor.Executor;
import io.mycat.sqlEngine.executor.logicExecutor.ExecutorType;
import io.mycat.sqlEngine.persistent.PersistentManager;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
/**
 * @author Junwen Chen
 **/
public class DbConsole {

  final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(DbConsole.class);
  final RootSessionContext context;
  final PhysicsExecutorRunner runner = new PhysicsExecutorRunner();
  final ComplierContext complierContext;
  DbSchema currentSchema;

  public DbConsole() {
    context = new RootSessionContext();
    complierContext = new ComplierContext(context);
  }

  public Iterator<Executor> input(String sql) {
    Iterator<SQLStatement> statementIterator = SQLParser.INSTANCE.parse(sql).iterator();
    return new Iterator<Executor>() {
      @Override
      public boolean hasNext() {
        return statementIterator.hasNext();
      }

      @Override
      public Executor next() {
        SQLStatement statement = statementIterator.next();
        context.rootType = ExecutorType.QUERY;
        StatementDispatcher statementDispatcher = new StatementDispatcher(DbConsole.this);
        statement.accept(statementDispatcher);
        return response(statementDispatcher);
      }

      private Executor response(StatementDispatcher mycatStatementVisitor) {
        Executor consoleResult = mycatStatementVisitor.getConsoleResult();
        return (consoleResult != null) ? runner.run(DbConsole.this) : EmptyExecutor.INSTACNE;
      }
    };
  }

  public static void main(String[] args) throws IOException, URISyntaxException {
    PrintStream out = System.out;
    DbConsole console = DbSchemaManager.INSTANCE.createConsole();
    String text = new String(Files.readAllBytes(
        Paths.get(DbConsole.class.getClassLoader().getResource("test.txt").toURI())
            .toAbsolutePath()));
    Iterator<Executor> iterator = console.input(text);
    int id = 1;
    while (iterator.hasNext()) {
      Executor result = iterator.next();
      if (result == null) {
        continue;
      }
      out.println("id:" + id);
      ++id;
      if (result instanceof EmptyExecutor) {
        out.println("ok");
        continue;
      }
      String columnText = Arrays.stream(result.columnDefList()).map(i -> i.getColumnName()).collect(
          Collectors.joining("|","|","|"));

      out.println(columnText);
      Iterator<Object[]> rowIterator = result;
      while (rowIterator.hasNext()) {
        Object[] rowList = rowIterator.next();
        try {
          String rowText = Arrays.asList(rowList).stream().map(i -> Objects.toString(i))
              .collect(Collectors.joining("|", "|", "|"));
          out.println(rowText);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    System.out.println();
  }

  public boolean createSchema(String databaseName) {
    DbSchema mycatSchema = DbSchemaManager.INSTANCE.schemas
        .computeIfAbsent(databaseName, DbSchema::new);
    if (currentSchema == null) {
      currentSchema = mycatSchema;
    }
    return true;
  }

  public Executor showDatabase() {
    BaseColumnDefinition[] columnList = new BaseColumnDefinition[]{
        new BaseColumnDefinition("Database", String.class)};
    List<String[]> list = new ArrayList<>();
    for (String database : DbSchemaManager.INSTANCE.schemas.keySet()) {
      list.add(new String[]{database});
    }
    return new DefExecutor(columnList, list);
  }

  public void createTable(DbTable table) {
    currentSchema.createTable(table);
    PersistentManager.INSTANCE.createPersistent(table, null, Collections.emptyMap());
  }

  public void dropDatabase(String databaseName) {
    DbSchemaManager.INSTANCE.schemas.remove(databaseName);
  }

  public void dropTable(String tableGroupName) {
    currentSchema.dropTable(tableGroupName);
  }

  public void dropTable(List<String> nameList) {
    currentSchema.dropTable(nameList);
  }

  public DbSchema getCurrentSchema() {
    return currentSchema;
  }

  public RootSessionContext getContext() {
    return context;
  }

  public ComplierContext getComplierContext() {
    return complierContext;
  }

}