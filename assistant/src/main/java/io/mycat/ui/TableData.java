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
@AllArgsConstructor
public class TableData {
  int columnCount;
  ResultSetMetaData resultSetMetaData;
  List<String[]> dataList;
}
