package io.mycat.config.route;

/**
 * @author jamie12221
 *  date 2019-05-04 01:03
 **/
public class ShardingColumn {
  String column;
  ShardingColumn columns;

  public String getColumn() {
    return column;
  }

  public void setColumn(String column) {
    this.column = column;
  }

  public ShardingColumn getColumns() {
    return columns;
  }

  public void setColumns(ShardingColumn columns) {
    this.columns = columns;
  }
}
