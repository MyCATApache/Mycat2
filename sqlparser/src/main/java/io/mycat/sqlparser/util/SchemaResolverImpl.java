package io.mycat.sqlparser.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SchemaResolverImpl implements SchemaResolver {

  final Map<String, SchemaRef> map = new HashMap<>();
  String currentSchema = "mycat";

  public SchemaResolverImpl() {
    String schemaName = "mycat";
    SchemaRef schemaRef = new SchemaRef(schemaName);
    TableRef tableRef = new TableRef("travelrecord");
    schemaRef.addTableRef(tableRef);
    addSchemaRef(schemaRef);

    tableRef.addColumn("id");
    tableRef.addColumn("user_id");
  }

  public void useSchema(String schema) {
    currentSchema = schema;
  }

  public void addSchemaRef(SchemaRef schemaRef) {
    map.put(schemaRef.schemaName, schemaRef);
  }

  @Override
  public TableRef find(String schema, String table) {
    return(map.get(schema == null ? currentSchema : schema))
        .findTable(table);
  }
}