package io.mycat.proxy.packet;

import io.mycat.proxy.session.MySQLServerSession;
import io.mycat.util.JavaClassToMySQLTypeUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.ListIterator;

/**
 * @author jamie12221
 * @date 2019-05-08 16:39
 **/
public class ResultSet {
  int columnCount;
  String[] columnNames;
  int[] types;
  ArrayList<byte[][]> rows;
  public ResultSet(int columnCount) {
    this.columnCount = columnCount;
    this.columnNames = new String[columnCount];
    this.types = new int[columnCount];
    this.rows = new ArrayList<>();
  }
  public void putColumnName(int index,String columnName){
    columnNames[index] = columnName;
  }
  public void putColumnType(int index,int type){
    types[index] = type;
  }
  public void putColumnType(int index,Class c){
    types[index] = JavaClassToMySQLTypeUtil.getMySQLType(c);
  }
  public void add(byte[][] row){
    rows.add(row);
  }
  void write(MySQLServerSession s,boolean binaryRow,boolean end) throws IOException {
    s.writeColumnCount(columnCount);
    for (int i = 0; i < columnCount; i++) {
      s.writeColumnDef(columnNames[i],types[i]);
    }
    columnNames = null;
    types = null;
    ListIterator<byte[][]> iterator = rows.listIterator();
    while (iterator.hasNext()){
      byte[][] next = iterator.next();
      iterator.set(null);
      if (!binaryRow){
        s.writeTextRowPacket(next);
      }else {
        s.writeBinaryRowPacket(next);
      }
    }
    if (end){
      s.writeRowEndPacket(true,false);
    }
  }
}
