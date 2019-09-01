package io.mycat.sqlparser.util;

import java.util.HashMap;
import java.util.Map;

public class SchemaRef {

   String schemaName;
  private Map<String,TableRef> map = new HashMap<>();
  public SchemaRef(String schemaName) {
    this.schemaName = schemaName;
  }

  public TableRef findTable(String table) {
    return map.get(table);
  }

  public void addTableRef(TableRef tableRef) {
    map.put(tableRef.name,tableRef);
    tableRef.setSchemaRef(this);
  }
}