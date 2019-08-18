package io.mycat.sqlparser.util;

import java.util.HashMap;
import java.util.Map;

public class TableRef implements SObject {

  SchemaRef schemaRef;
  final String name;
  final Map<String,ColumnRef> columnRefMap = new HashMap<>();

  public TableRef(String name) {
    this.name = name;
  }


  @Override
  public String sql() {
    return name;
  }

  public String qualifiedName() {
    return schemaRef.schemaName+"."+name;
  }

  public String qualifiedColumnName(String columnName){
     if(columnRefMap.containsKey(columnName)){
       return schemaRef.schemaName+"."+name+"."+columnName;
     }else {
       throw new UnsupportedOperationException();
     }
  }

  public SchemaRef getSchemaRef() {
    return schemaRef;
  }

  public void setSchemaRef(SchemaRef schemaRef) {
    this.schemaRef = schemaRef;
  }

  public void addColumn(String id) {
    columnRefMap.put(id,new ColumnRef(id));
  }
}