package io.mycat.ui;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * resultSet
 *
 * @author wyzhang
 * @date 2021/8/12 12:01
 */
@Data
public class TableData {
  int columnCount;
  ResultSetMetaData resultSetMetaData;
  List<String> columnNames;
  List<String[]> dataList;

  public TableData(List<String> columnNames, List<String[]> dataList) {
    this.columnCount = columnNames.size();
    this.columnNames = columnNames;
    this.dataList = dataList;
  }

  public TableData(int columnCount, ResultSetMetaData resultSetMetaData, List<String[]> dataList) {
    this.columnCount = columnCount;
    this.resultSetMetaData = resultSetMetaData;
    this.dataList = dataList;
  }
}
