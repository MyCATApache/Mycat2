package io.mycat.ui;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * 结果集打印机.将结果集中的数据打印成表格.
 */
public class ResultSetPrinter {
    public static TableData getTable(ResultSet rs) throws SQLException {
        StringBuilder text = new StringBuilder();
        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        // 获取列数
        int ColumnCount = resultSetMetaData.getColumnCount();
        // 保存当前列最大长度的数组
        int[] columnMaxLengths = new int[ColumnCount];
        // 缓存结果集,结果集可能有序,所以用ArrayList保存变得打乱顺序.
        ArrayList<String[]> results = new ArrayList<>();

        // 按行遍历
        while (rs.next()) {
            // 保存当前行所有列
            String[] columnStr = new String[ColumnCount];
            // 获取属性值.
            for (int i = 0; i < ColumnCount; i++) {
                // 获取一列
                columnStr[i] = rs.getString(i + 1);
            }
            // 缓存这一行.
            results.add(columnStr);
        }

        return new TableData(ColumnCount, resultSetMetaData, results);
    }
}
