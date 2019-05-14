package io.mycat.logTip;

/**
 * @author jamie12221
 * @date 2019-05-11 21:53
 *
 * 本类管理所有DataSource相关的日志或者异常提示
 **/
public enum DataSourceTip implements LogTip {
  CREATE_DATASOURCE_SUCCESS("%s dataSource create successful"),
  CREATE_DATASOURCE_FAIL("%s dataSource create fail :%s"),
  READ_CHARSET_SUCCESS("%s dataSource read charset successful"),
  READ_CHARSET_FAIL("%s dataSource read charset fail"),
  UNKNOWN_IDLE_CLOSE("mysql session is idle but it closed");
  String message;

  DataSourceTip(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }
}
