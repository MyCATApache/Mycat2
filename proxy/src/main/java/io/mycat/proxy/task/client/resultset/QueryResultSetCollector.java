package io.mycat.proxy.task.client.resultset;

import com.google.common.collect.Lists;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author jamie12221
 * @date 2019-05-11 14:44 文本结果集收集类
 **/
public class QueryResultSetCollector implements TextResultSetTransforCollector,
    Iterable<Object[]> {

  protected ArrayList[] result;
  protected ColumnDefPacket[] columns;
  protected int columnCount = 0;

  @Override
  public void addValue(int columnIndex) {
    result[columnIndex].add(null);
  }

  @Override
  public void addString(int columnIndex, String value) {
    result[columnIndex].add(value);
  }

  @Override
  public void addValue(int columnIndex, long value, boolean isNUll) {
    result[columnIndex].add(value);
  }

  @Override
  public void addValue(int columnIndex, double value, boolean isNUll) {
    result[columnIndex].add(value);
  }

  @Override
  public void addBlob(int columnIndex, byte[] value) {
    result[columnIndex].add(value);
  }

  @Override
  public void addValue(int columnIndex, byte value, boolean isNUll) {
    result[columnIndex].add(value);
  }

  @Override
  public void addDecimal(int columnIndex, BigDecimal value) {
    result[columnIndex].add(value);
  }

  public void collectColumnList(ColumnDefPacket[] packets) {
    columns = packets;
    result = new ArrayList[packets.length];
    for (int i = 0; i < packets.length; i++) {
      result[i] = new ArrayList();
      columnCount++;
    }
  }


  @Override
  public Iterator<Object[]> iterator() {
    ArrayList[] res = this.result;
    if (res.length == 0) {
      return Collections.emptyIterator();
    }
    int size = result[0].size();
    if (size == 0) {
      return Collections.emptyIterator();
    }
    return new Iterator<Object[]>() {
      ArrayList[] result = res;
      int index;

      @Override
      public boolean hasNext() {

        return index < size;
      }

      @Override
      public Object[] next() {
        Object[] objects = new Object[columnCount];
        for (int i = 0; i < columnCount; i++) {
          objects[i] = result[i].get(index);
        }
        index++;
        return objects;
      }
    };
  }

  public List<Map<String,Object>> toList() {
    ArrayList[] res = this.result;
    if(res.length == 0 || result[0].size() == 0) {
        return Collections.emptyList();
    }
    List<Map<String, Object>> list = Lists.newArrayList();
    int size = result[0].size();
    for(int index = 0 ; index < size; index ++){
      Map<String, Object> map = new HashMap<>();
      list.add(map);
    }
    for (int i = 0; i < columns.length; i++) {
      String columnNameString = columns[i].getColumnNameString();
      for(int index = 0 ; index < size; index ++){
        list.get(index).put(columnNameString, res[i].get(index));
      }
    }
    return list;
  }
  @Override
  public String toString() {
    if(this.result.length == 0 || result[0].size() == 0) {
      return "";
    }
    for(int index = 0 ; index <  result[0].size(); index ++) {
      Object[] objects = new Object[columnCount];
      for (int i = 0; i < columnCount; i++) {
        objects[i] = result[i].get(index);
      }
      logger.info("result[{}] {}" ,index,  Arrays.toString(objects));
    }
    return super.toString();
  }
}
