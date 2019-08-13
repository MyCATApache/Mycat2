package io.mycat.api.collector;

import io.mycat.beans.mysql.packet.ColumnDefPacket;
import java.math.BigDecimal;

/**
 * @author jamie12221
 *  date 2019-05-11 14:44 文本结果集收集类
 **/

public interface ResultSetCollector {

  void onResultSetStart();

  void onResultSetEnd();

  void onRowStart();

  void onRowEnd();

  void addNull(int columnIndex);

  void addString(int columnIndex, String value);

  void addBlob(int columnIndex, byte[] value);


  void addValue(int columnIndex, long value, boolean isNUll);

  void addValue(int columnIndex, double value, boolean isNUll);

  void addValue(int columnIndex, byte value, boolean isNUll);


  void addDecimal(int columnIndex, BigDecimal value);

  void collectColumnList(ColumnDefPacket[] packets);

  void addDate(int columnIndex, java.util.Date date);
}
