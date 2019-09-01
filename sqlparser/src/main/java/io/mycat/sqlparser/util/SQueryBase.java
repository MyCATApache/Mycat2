package io.mycat.sqlparser.util;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SQueryBase {

  private final TableRef from;
  private final List<WhereCondition> columnRefValue = new ArrayList<>(3);
  private final List<ColumnRef> selectItems = new ArrayList<>();

  public SQueryBase(TableRef from) {
    this.from = from;
  }

  public void addCondition(ColumnRef columnRef, java.lang.Object value){
    columnRefValue.add(new WhereCondition(columnRef,value));
  }
  public void addSelectItem(ColumnRef columnRef){
    selectItems.add(columnRef);
  }
  public String getSQL() {
    String selectItems = this.selectItems.isEmpty() ? "*"
        : this.selectItems.stream().map(s -> s.sql()).collect(Collectors.joining(" , "));
    String where = this.columnRefValue.isEmpty() ? "" : this.columnRefValue.stream()
        .map(c -> MessageFormat.format(" {0} = {1}", c.getColumnRef().sql(), c.getValue())).collect(
            Collectors.joining("and ", "where", " "));
    return MessageFormat.format("select {0} from {1} {2}", selectItems,from.sql(), where);
  }

  public static void main(String[] args) {
    SQueryBase sQueryContext = new SQueryBase(new TableRef("travelrecord"));
    sQueryContext.addSelectItem(new ColumnRef("id"));
    sQueryContext.addSelectItem(new ColumnRef("age"));
    sQueryContext.addCondition(new ColumnRef("id"),"1");
    String sql = sQueryContext.getSQL();
    System.out.println(sql);
  }
  private static class WhereCondition{
    final ColumnRef columnRef;
    final Object value;

    public WhereCondition(ColumnRef columnRef, Object value) {
      this.columnRef = columnRef;
      this.value = value;
    }

    public ColumnRef getColumnRef() {
      return columnRef;
    }

    public Object getValue() {
      return value;
    }
  }
}