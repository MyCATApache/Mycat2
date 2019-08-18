package io.mycat.sqlparser.util.aliasResolver;


import io.mycat.sqlparser.util.SchemaResolver;
import io.mycat.sqlparser.util.TableRef;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

public class TableAliasResolverImpl implements PrimitiveNameResolver {

  final SchemaResolver schemaResolver;
  final LinkedList<Map<String, TableRef>> stack = new LinkedList<>();
  Map<String, TableRef> current = null;

  public TableAliasResolverImpl(
      SchemaResolver schemaResolver) {
    this.schemaResolver = schemaResolver;
  }

  @Override
  public void newScope() {
    if (current != null) {
      stack.add(current);
      current = null;
    }
  }

  @Override
  public void endScope() {
    while (!stack.isEmpty()) {
      current = stack.pollFirst();
      if (current != null) {
        break;
      }
    }
  }

  @Override
  public void record(String schema, String table, String alias) {
    if (current == null) {
      current = new HashMap<>();
    }
    TableRef tableRef = this.schemaResolver.find(schema, table);
    Objects.requireNonNull(tableRef);
    current.put(alias, tableRef);
  }

  @Override
  public String resolveQualifiedTableName(String identifier) {
    Iterator<Map<String, TableRef>> iterator = stack.iterator();
    while (iterator.hasNext()) {
      Map<String, TableRef> next = iterator.next();
      if (next != null) {
        TableRef tableRef = next.get(identifier);
        return tableRef.qualifiedName();
      }
    }
    TableRef tableRef = schemaResolver.find(null, identifier);
    return tableRef==null?identifier:tableRef.qualifiedName();
  }

  public String resolveQualifiedColumnName(String identifier) {
    Iterator<Map<String, TableRef>> iterator = stack.iterator();
    while (iterator.hasNext()) {
      Map<String, TableRef> next = iterator.next();
      if (next != null) {
        TableRef tableRef = next.get(identifier);
        return tableRef.qualifiedColumnName(identifier);
      }
    }
    TableRef tableRef = schemaResolver.find(null, identifier);
    return tableRef==null?identifier:tableRef.qualifiedColumnName(identifier);
  }





}