package io.mycat.sqlparser.util.dataLayout;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLValuableExpr;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DeleteDataAffinity {

  MySqlInsertStatement statement;
  private final DataLayoutMapping dataLayoutMapping;
  private final int partitionColumnIndex;
  final Map<Integer, List<List<SQLExpr>>> groupBy = new HashMap<>();

  public DeleteDataAffinity(int partitionColumnIndex, DataLayoutMapping dataLayoutMapping) {
    this.partitionColumnIndex = partitionColumnIndex;
    this.dataLayoutMapping = dataLayoutMapping;
  }

  public void insert(List<SQLExpr> values) {
    SQLExpr sqlExpr = values.get(partitionColumnIndex);
    String value = ((SQLValuableExpr) sqlExpr).getValue().toString();
    Integer dataNodeIndex = dataLayoutMapping.calculate(value);
    List<List<SQLExpr>> quque = groupBy.computeIfAbsent(dataNodeIndex, k -> new LinkedList<>());
    quque.add(values);
  }

  public void saveParseTree(MySqlInsertStatement statement) {
    this.statement = statement;
  }
}