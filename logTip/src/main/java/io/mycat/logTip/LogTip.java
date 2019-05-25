package io.mycat.logTip;

import java.util.Objects;

/**
 * @author jamie12221
 *  date 2019-05-14 11:04
 **/
public interface LogTip {

  String getMessage();

  default String getMessage(Object... args) {
    for (int i = 0; i < args.length; i++) {
      args[i] = Objects.toString(args[i]);
    }
    try {
      return String.format(this.getMessage(), args);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return "";
  }
}
