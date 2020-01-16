/**
 * Copyright (C) <2020>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.api.collector;

import java.util.*;
import java.util.Map.Entry;

/**
 * @author jamie12221
 * date 2019-05-22 23:18
 **/
public class CollectorUtil {

  /**
   * a collector for only one result set
   * @return
   */
  public static TextResultSetTransforCollector newOneResultSetTransforCollector() {
    return new TextResultSetTransforCollector(new OneResultSetCollector());
  }

  /**
   * transform a collector to iterable
   * @return
   */
  public static Iterable<Object[]> iterable(OneResultSetCollector collector) {
    return collector;
  }

  public static <T> T getValue(OneResultSetCollector collector, String columnName, Object[] row) {
    Integer index = collector.columns.get(columnName);
    Objects.requireNonNull(index);
    return (T) row[index];
  }

  /**
   * transform a collector to Iterator
   * @return
   */
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

  /**
   * transform a collector to simple List<Map<Sting,Object>>
   * @return
   */
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
