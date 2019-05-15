package io.mycat.proxy.task.client;

import java.math.BigDecimal;

/**
 * @author jamie12221
 * @date 2019-05-15 11:23
 **/
public interface ResultSetCollector {

  default void addValue(int columnIndex) {

  }

  default void addValue(int columnIndex, String value) {

  }


  default void addValue(int columnIndex, long value) {

  }

  default void addValue(int columnIndex, double value) {

  }


  default void addValue(int columnIndex, byte[] value) {

  }

  default void addValue(int columnIndex, byte value) {

  }

  default void addValue(int columnIndex, BigDecimal value) {

  }

  default void addValue(int columnIndex, java.util.Date date) {

  }

}
