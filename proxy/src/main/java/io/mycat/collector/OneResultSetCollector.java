package io.mycat.collector;

import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.util.StringUtil;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jamie12221
 * @date 2019-05-22 01:17
 **/
public class OneResultSetCollector implements ResultSetCollector, Iterable<Object[]> {

  private static final Logger LOGGER = LoggerFactory.getLogger(StringUtil.class);
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
      LOGGER.info("result[{}] {}", index, Arrays.toString(objects));
    }
    return super.toString();
  }

  @Override
  public Iterator<Object[]> iterator() {
    return CollectorUtil.iterator(this);
  }
}
