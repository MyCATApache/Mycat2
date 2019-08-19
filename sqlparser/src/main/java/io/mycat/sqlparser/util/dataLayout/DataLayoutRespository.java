package io.mycat.sqlparser.util.dataLayout;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.repository.SchemaObject;
import io.mycat.sqlparser.util.MycatSchemaRespository;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataLayoutRespository {

  final MycatSchemaRespository schemaRespository = new MycatSchemaRespository();

  final Map<SchemaObject, SQLColumnDefinition> schema2PartitionColumn = new HashMap<>();
  final Map<SchemaObject, DataLayoutMapping> schema2DataLayout = new HashMap<>();


  public int findPartitionColumnIndex(SQLExprTableSource tableSource, List<SQLExpr> columns) {
    SQLColumnDefinition defaultColumn = tableSource.findColumn("id");
    ;
    SQLColumnDefinition partitionColumnDefinition = schema2PartitionColumn.getOrDefault(tableSource,defaultColumn);
    for (int i = 0; i < columns.size(); i++) {
      SQLColumnDefinition column = schemaRespository.findColumn(tableSource, columns.get(i));
      if (partitionColumnDefinition==column){
        return i;
      }
    }
    return -1;
  }

  public InsertDataAffinity getTableDataLayout(SchemaObject tableSchema) {
    return null;
  }

  public InsertDataAffinity createInsertDataAffinity(String schema, String table) {
    return null;
  }

  public int calculate(SQLColumnDefinition partitionColumn, String toString) {

    return 0;
  }

  public InsertDataAffinity getTableDataLayout(SQLExprTableSource tableSchema, List<SQLExpr> columns) {
    return new InsertDataAffinity(findPartitionColumnIndex(tableSchema, columns),
        getDataLayoutMapping(tableSchema.getSchemaObject()));
  }

  private DataLayoutMapping getDataLayoutMapping(SchemaObject tableSchema) {
    return schema2DataLayout.getOrDefault(tableSchema, new DataLayoutMapping() {
      @Override
      public int calculate(String columnValue) {
        return columnValue.hashCode();
      }
    });
  }
}