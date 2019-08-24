//package io.mycat.sqlparser.util.dataLayout;
//
//import com.alibaba.fastsql.sql.ast.SQLExpr;
//import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
//import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
//import com.alibaba.fastsql.sql.repository.SchemaObject;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//public class DataLayoutRespository {
//
//  final MycatSchemaRespository schemaRespository;
//
//  final Map<SchemaObject, SQLColumnDefinition> schema2PartitionColumn = new HashMap<>();
//  final Map<SchemaObject, DataLayoutMapping> schema2DataLayout = new HashMap<>();
//
//  public DataLayoutRespository(MycatSchemaRespository schemaRespository) {
//    this.schemaRespository = schemaRespository;
//  }
//
//  public SQLColumnDefinition getPartitionColumn(SchemaObject schemaObject) {
//    return schema2PartitionColumn.get(schemaObject);
//  }
//
//  public int findPartitionColumnIndex(SQLExprTableSource tableSource, List<SQLExpr> columns) {
//    SQLColumnDefinition defaultColumn = tableSource.findColumn("id");
//    ;
//    SQLColumnDefinition partitionColumnDefinition = schema2PartitionColumn
//        .getOrDefault(tableSource, defaultColumn);
////    for (int i = 0; i < columns.size(); i++) {
////      SQLColumnDefinition column = schemaRespository.findColumn(tableSource, columns.get(i));
////      if (partitionColumnDefinition == column) {
////        return i;
////      }
////    }
//    return -1;
//  }
//
//  public InsertDataAffinity getInsertTableDataLayout(SchemaObject tableSchema) {
//    return null;
//  }
//
//  public InsertDataAffinity createInsertDataAffinity(String schema, String table) {
//    return null;
//  }
//
//  public int calculate(SQLColumnDefinition partitionColumn, String toString) {
//
//    return 0;
//  }
//
//
//  public InsertDataAffinity getDeleteTableDataLayout(SQLExprTableSource tableSchema,
//      List<SQLExpr> columns) {
//    return new InsertDataAffinity(findPartitionColumnIndex(tableSchema, columns),
//        getDataLayoutMapping(tableSchema.getSchemaObject()));
//  }
//
//  public InsertDataAffinity getInsertTableDataLayout(SQLExprTableSource tableSchema,
//      List<SQLExpr> columns) {
//    return new InsertDataAffinity(findPartitionColumnIndex(tableSchema, columns),
//        getDataLayoutMapping(tableSchema.getSchemaObject()));
//  }
//
//  public DataLayoutMapping getDataLayoutMapping(SchemaObject tableSchema) {
//    return schema2DataLayout.getOrDefault(tableSchema, new DataLayoutMapping() {
//      @Override
//      public int calculate(String columnValue) {
//        return columnValue.hashCode();
//      }
//    });
//  }
//}