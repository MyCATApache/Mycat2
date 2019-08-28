package cn.lightfish.sql.persistent;

import cn.lightfish.sql.schema.MycatTable;
import java.util.List;

public class InsertPersistent {

  private final MycatTable table;
  private final List<Object[]> rows;

  public InsertPersistent(MycatTable table, List<Object[]> rows) {
    this.table = table;
    this.rows = rows;
  }

  public void insert(Object[] row) {
    rows.add(row);
  }
}