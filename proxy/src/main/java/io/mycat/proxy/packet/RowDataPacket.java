package io.mycat.proxy.packet;

/**
 * @author jamie12221
 * @date 2019-05-07 23:22
 **/
public class RowDataPacket {
  byte[][] row;

  public RowDataPacket(int count) {
    this.row = new byte[count][];
  }
//  public void put(int index,String v, Charset charset){
//    row[index] = v.getBytes(charset);
//  }
//  public void put(int index,long v){
//    row[index] = By;
//  }
}
