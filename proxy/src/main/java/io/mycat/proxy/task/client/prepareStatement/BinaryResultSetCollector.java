package io.mycat.proxy.task.client.prepareStatement;

import io.mycat.beans.mysql.packet.ColumnDefPacket;
import java.math.BigDecimal;
import java.util.ArrayList;

/**
 * @author jamie12221
 * @date 2019-05-15 10:38
 **/
public class BinaryResultSetCollector extends BinaryResultSetTransforCollector {

  protected ArrayList[] result;
  protected ColumnDefPacket[] columns;
  protected int columnCount = 0;


  @Override
  public void onResultSetEnd() {
    logger.debug("onResultSetEnd");
  }


  protected void addValue(int columnIndex) {

  }

  protected void addValue(int columnIndex, String value) {

  }


  protected void addValue(int columnIndex, long value) {

  }

  protected void addValue(int columnIndex, double value) {

  }


  protected void addValue(int columnIndex, byte[] value) {

  }

  protected void addValue(int columnIndex, byte value) {

  }

  protected void addValue(int columnIndex, BigDecimal value) {

  }

  protected void addValue(int columnIndex, java.util.Date date) {

  }

}
