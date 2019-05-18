package io.mycat.logTip;

import io.mycat.MycatExpection;

/**
 * @author jamie12221
 * @date 2019-05-06 00:38 本类管理所有Router相关的日志或者异常提示
 **/
public enum RouteNullChecker {
  CHECK_MYCAT_SCHEMA_EXIST(0, "%s schema is not exist "),
  CHECK_MYCAT_TABLE_EXIST(1, "%s table is not exist "),
  CHECK_MYCAT_MULTI_TABLE_IN_DB_IN_MULTI_SERVER(2, "%s tables is diff "),
  CHECK_MYCAT_UNSUPPORT_WITH_SCHEMA(3, "sql can not with schema"),
  ;
  String format;
  int errorCode;

  RouteNullChecker(int errorCode, String format) {
    this.errorCode = errorCode;
    this.format = format;
  }

  public void check(boolean contition) throws RuntimeException {
    if (!contition) {
      throw new MycatExpection(errorCode, this.format);
    }
  }

  public void check(String o, boolean contition) throws RuntimeException {
    if (!contition) {
      throw new MycatExpection(errorCode, String.format(this.format, o));
    }
  }

  public void check(String arg1, String arg2, boolean contition) throws RuntimeException {
    if (!contition) {
      throw new MycatExpection(errorCode, String.format(this.format, arg1, arg2));
    }
  }
}
