/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.api.collector;

import io.mycat.beans.mysql.packet.ColumnDefPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

/**
 * @author jamie12221
 *  date 2019-05-22 01:17
 *  a simple proxy collector as map
 **/
public class OneResultSetCollector implements ResultSetCollector, Iterable<Object[]>{

  private static final Logger LOGGER = LoggerFactory.getLogger(OneResultSetCollector.class);
  ArrayList[] result;
  Map<String, Integer> columns;
  int columnCount = 0;

  @Override
  public void onResultSetStart() {

  }

  @Override
  public void onResultSetEnd() {

  }

  @Override
  public void onRowStart() {

  }

  @Override
  public void onRowEnd() {

  }

  @Override
  public void addNull(int columnIndex) {
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
    columns = new HashMap<>(packets.length);
    for (int i = 0; i < packets.length; i++) {
      ColumnDefPacket packet = packets[i];
      columns.put(packet.getColumnNameString(), i);
    }
    result = new ArrayList[packets.length];
    for (int i = 0; i < packets.length; i++) {
      result[i] = new ArrayList();
      columnCount++;
    }
  }

  @Override
  public void addDate(int columnIndex, Date date) {
    result[columnIndex].add(date);
  }


  @Override
  public String toString() {
    if (this.result.length == 0 || result[0].size() == 0) {
      return "";
    }
    for (int index = 0; index < result[0].size(); index++) {
      Object[] objects = new Object[columnCount];
      for (int i = 0; i < columnCount; i++) {
        objects[i] = result[i].get(index);
      }
      LOGGER.debug("result[{}] {}", index, Arrays.toString(objects));
    }
    return super.toString();
  }

  @Override
  public Iterator<Object[]> iterator() {
    return CollectorUtil.iterator(this);
  }

}
