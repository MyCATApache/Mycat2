package io.mycat.beans.mysql;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jamie12221
 * @date 2019-05-23 17:22
 **/
public class MySQLVariables {

  boolean mysql8;

  public MySQLVariables(boolean mysql8) {
    this.mysql8 = mysql8;
  }

  final Map<String, Object> map = new HashMap<>();
}
