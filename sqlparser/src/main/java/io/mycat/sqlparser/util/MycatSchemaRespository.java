package io.mycat.sqlparser.util;


import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLCreateSequenceStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLDropSequenceStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectItem;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
import com.alibaba.fastsql.sql.repository.Schema;
import com.alibaba.fastsql.sql.repository.SchemaObject;
import com.alibaba.fastsql.sql.repository.SchemaRepository;
import com.alibaba.fastsql.sql.repository.SchemaResolveVisitor.Option;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class MycatSchemaRespository {

  final SchemaRepository schemaRepository = new SchemaRepository(DbType.mysql);


  public String getDefaultSchemaName() {
    return schemaRepository.getDefaultSchemaName();
  }

  public void setDefaultSchema(String name) {
    schemaRepository.setDefaultSchema(name);
  }

  public Schema findSchema(String schema) {
    return schemaRepository.findSchema(schema);
  }

  public Schema getDefaultSchema() {
    return schemaRepository.getDefaultSchema();
  }

  public void setDefaultSchema(Schema schema) {
    schemaRepository.setDefaultSchema(schema);
  }

  public SchemaObject findTable(String tableName) {
    return schemaRepository.findTable(tableName);
  }

  public SchemaObject findTableOrView(String tableName) {
    return schemaRepository.findTableOrView(tableName);
  }

  public SchemaObject findTableOrView(String tableName,
      boolean onlyCurrent) {
    return schemaRepository.findTableOrView(tableName, onlyCurrent);
  }

  public Collection<Schema> getSchemas() {
    return schemaRepository.getSchemas();
  }

  public SchemaObject findFunction(String functionName) {
    return schemaRepository.findFunction(functionName);
  }

  public void acceptDDL(String ddl) {
    schemaRepository.acceptDDL(ddl);
  }

  public void acceptDDL(String ddl, String dbType) {
    schemaRepository.acceptDDL(ddl, DbType.mysql);
  }

  public void accept(SQLStatement stmt) {
    schemaRepository.accept(stmt);
  }

  public boolean isSequence(String name) {
    return schemaRepository.isSequence(name);
  }

  public SchemaObject findTable(
      SQLTableSource tableSource, String alias) {
    return schemaRepository.findTable(tableSource, alias);
  }

  public SQLColumnDefinition findColumn(
      SQLTableSource tableSource,
      SQLSelectItem selectItem) {
    return schemaRepository.findColumn(tableSource, selectItem);
  }

  public SQLColumnDefinition findColumn(
      SQLTableSource tableSource,
      SQLExpr expr) {
    return schemaRepository.findColumn(tableSource, expr);
  }

  public SchemaObject findTable(
      SQLTableSource tableSource,
      SQLSelectItem selectItem) {
    return schemaRepository.findTable(tableSource, selectItem);
  }

  public SchemaObject findTable(
      SQLTableSource tableSource,
      SQLExpr expr) {
    return schemaRepository.findTable(tableSource, expr);
  }

  public Map<String, SchemaObject> getTables(
      SQLTableSource x) {
    return schemaRepository.getTables(x);
  }

  public int getTableCount() {
    return schemaRepository.getTableCount();
  }

  public Collection<SchemaObject> getObjects() {
    return schemaRepository.getObjects();
  }

  public int getViewCount() {
    return schemaRepository.getViewCount();
  }

  public void resolve(SQLSelectStatement stmt,
      Option... options) {
    schemaRepository.resolve(stmt, options);
  }

  public void resolve(SQLStatement stmt,
      Option... options) {
    schemaRepository.resolve(stmt, options);
  }

  public String resolve(String input) {
    return schemaRepository.resolve(input);
  }

  public String console(String input) {
    return schemaRepository.console(input);
  }

  public SchemaObject findTable(
      SQLName name) {
    return schemaRepository.findTable(name);
  }

  public SchemaObject findTable(
      SQLExprTableSource x) {
    return schemaRepository.findTable(x);
  }

  public boolean acceptCreateSequence(
      SQLCreateSequenceStatement x) {
    return schemaRepository.acceptCreateSequence(x);
  }

  public boolean acceptDropSequence(SQLDropSequenceStatement x) {
    return schemaRepository.acceptDropSequence(x);
  }

  public void dropSchema(String simpleName) {

  }

  public SQLColumnDefinition findPartitionColumn(List<SQLExpr> columns) {

    return null;
  }

  public int findPartitionColumnIndex(List<SQLExpr> columns) {
    return 0;
  }
}