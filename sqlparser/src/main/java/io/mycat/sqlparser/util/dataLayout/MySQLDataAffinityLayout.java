package io.mycat.sqlparser.util.dataLayout;


import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MySQLDataAffinityLayout {

  public Map<Integer, String> insertDataAffinity(InsertDataAffinity insertDataAffinity) {
    MySqlInsertStatement statement = insertDataAffinity.statement;
    Map<Integer, List<List<SQLExpr>>> groupBy = insertDataAffinity.groupBy;
    Map<Integer, String> result = new HashMap<>(groupBy.size());
    List<ValuesClause> valuesList = statement.getValuesList();
    for (Entry<Integer, List<List<SQLExpr>>> entry : groupBy.entrySet()) {
      valuesList.clear();
      for (List<SQLExpr> sqlExprs : entry.getValue()) {
        valuesList.add(new ValuesClause(sqlExprs));
      }
      result.put(entry.getKey(),statement.toString());
    }
    return result;
  }
}