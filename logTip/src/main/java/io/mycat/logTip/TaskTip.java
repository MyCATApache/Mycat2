package io.mycat.logTip;

/**
 * @author jamie12221
 * @date 2019-05-13 17:28
 **/
public enum TaskTip {
  UNSUPPORT_DEF_MAX_PACKET("unsupport max packet %d"),
  CLOSE_ERROR("channel close occur exception: %s"),
  MULTI_OK_REVEIVE_FAIL("can not receive enough multi ok packet"),
  UNKNOWN_FIELD_TYPE("unknown field type %s"),
  CREATE_MYSQL_BACKEND_CONNECTION_FAIL("create mysql backend fail");
  String message;

  TaskTip(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }

  public String getMessage(Object... args) {
    return String.format(this.getMessage(), args);
  }
}
