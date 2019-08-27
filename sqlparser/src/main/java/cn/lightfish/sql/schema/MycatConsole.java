package cn.lightfish.sql.schema;

import cn.lightfish.sql.ast.SQLParser;
import cn.lightfish.sql.ast.statement.StatementDispatcher;
import cn.lightfish.sql.context.RootSessionContext;
import cn.lightfish.sql.executor.DefExecutor;
import cn.lightfish.sql.executor.EmptyExecutor;
import cn.lightfish.sql.executor.PhysicsExecutorRunner;
import cn.lightfish.sql.executor.logicExecutor.Executor;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class MycatConsole {

  final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(MycatConsole.class);
  final RootSessionContext context = new RootSessionContext();
  final PhysicsExecutorRunner runner = new PhysicsExecutorRunner();
  MycatSchema currentSchema;

  public Iterator<Executor> input(String sql) {
    Iterator<SQLStatement> statementIterator = SQLParser.INSTANCE.parse(sql);
    return new Iterator<Executor>() {
      @Override
      public boolean hasNext() {
        return statementIterator.hasNext();
      }

      @Override
      public Executor next() {
        SQLStatement statement = statementIterator.next();
        StatementDispatcher statementDispatcher = new StatementDispatcher(MycatConsole.this);
        statement.accept(statementDispatcher);
        return response(statementDispatcher);
      }

      private Executor response(StatementDispatcher mycatStatementVisitor) {
        Executor consoleResult = mycatStatementVisitor.getConsoleResult();
        return (consoleResult != null) ? runner.run(MycatConsole.this) : EmptyExecutor.INSTACNE;
      }
    };
  }

  public static void main(String[] args) throws IOException, URISyntaxException {
    PrintStream out = System.out;
    MycatConsole console = MycatSchemaManager.INSTANCE.createConsole();
    String text = new String(Files.readAllBytes(
        Paths.get(MycatConsole.class.getClassLoader().getResource("test.txt").toURI())
            .toAbsolutePath()));
    Iterator<Executor> iterator = console.input(text);
    int id = 1;
    while (iterator.hasNext()) {
      Executor result = iterator.next();
      if (result == null) {
        continue;
      }

      out.println("-------------------------------------------------------------------------");
      out.println("id:" + id);
      ++id;
      if (result instanceof EmptyExecutor) {
        out.println("ok");
        continue;
      }
      String columnText = Arrays.stream(result.columnDefList()).map(i -> i.getColumnName()).collect(
          Collectors.joining("|"));

      char[] w = new char[columnText.length()];
      Arrays.fill(w, '-');

      out.println(new String(w));
      out.println(columnText);
      out.println(new String(w));
      Iterator<Object[]> rowIterator = result;
      while (rowIterator.hasNext()) {
        Object[] rowList = rowIterator.next();
        String rowText = Arrays.asList(rowList).stream().map(i -> i.toString())
            .collect(Collectors.joining("|", "|", "|"));
        out.println(rowText);
      }
    }
    System.out.println();
  }

  public boolean createSchema(String databaseName) {
    MycatSchema mycatSchema = MycatSchemaManager.INSTANCE.schemas
        .computeIfAbsent(databaseName, MycatSchema::new);
    if (currentSchema == null) {
      currentSchema = mycatSchema;
    }
    return true;
  }

  public Executor showDatabase() {
    SimpleColumnDefinition[] columnList = new SimpleColumnDefinition[]{
        new SimpleColumnDefinition("Database", String.class)};
    List<String[]> list = new ArrayList<>();
    for (String database : MycatSchemaManager.INSTANCE.schemas.keySet()) {
      list.add(new String[]{database});
    }
    return new DefExecutor(columnList, list);
  }

  public void createTable(MycatTable table) {
    currentSchema.createTable(table);
  }

  public void dropDatabase(String databaseName) {
    MycatSchemaManager.INSTANCE.schemas.remove(databaseName);
  }

  public void dropTable(String tableGroupName) {
    currentSchema.dropTable(tableGroupName);
  }

  public void dropTable(List<String> nameList) {
    currentSchema.dropTable(nameList);
  }

  public MycatSchema getCurrentSchema() {
    return currentSchema;
  }

  public RootSessionContext getContext() {
    return context;
  }
}