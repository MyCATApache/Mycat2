package cn.lightfish.sqlEngine.persistent;

import cn.lightfish.sqlEngine.schema.DbTable;
import java.util.List;

public class InsertPersistent {

  private final DbTable table;
  private final List<Object[]> rows;

  public InsertPersistent(DbTable table, List<Object[]> rows) {
    this.table = table;
    this.rows = rows;
  }

  public void insert(Object[] row) {
    rows.add(row);
  }
}