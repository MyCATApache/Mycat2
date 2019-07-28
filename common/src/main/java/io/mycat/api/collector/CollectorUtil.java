package io.mycat.api.collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * @author jamie12221
 *  date 2019-05-22 23:18
 **/
public class CollectorUtil {

  public static TextResultSetTransforCollector newOneResultSetTransforCollector() {
    return new TextResultSetTransforCollector(new OneResultSetCollector());
  }

  public static Iterable<Object[]> iterable(OneResultSetCollector collector) {
    return collector;
  }

  public static <T> T getValue(OneResultSetCollector collector, String columnName, Object[] row) {
    Integer index = collector.columns.get(columnName);
    Objects.requireNonNull(index);
    return (T) row[index];
  }

  public static Iterator<Object[]> iterator(OneResultSetCollector collector) {
    ArrayList[] res = collector.result;
    if (res.length == 0) {
      return Collections.emptyIterator();
    }
    int size = collector.result[0].size();
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
        Object[] objects = new Object[collector.columnCount];
        for (int i = 0; i < collector.columnCount; i++) {
          objects[i] = result[i].get(index);
        }
        index++;
        return objects;
      }
    };
  }

  public static List<Map<String, Object>> toList(OneResultSetCollector collector) {
    ArrayList[] res = collector.result;
    if (res.length == 0 || collector.result[0].size() == 0) {
      return Collections.emptyList();
    }
    List<Map<String, Object>> list = new ArrayList<>();
    int size = collector.result[0].size();
    for (int index = 0; index < size; index++) {
      Map<String, Object> map = new HashMap<>();
      list.add(map);
    }
    for (Entry<String, Integer> entry : collector.columns.entrySet()) {
      Integer i = entry.getValue();
      String columnNameString = entry.getKey();
      for (int index = 0; index < size; index++) {
        list.get(index).put(columnNameString, res[i].get(index));
      }
    }
    return list;
  }
}
