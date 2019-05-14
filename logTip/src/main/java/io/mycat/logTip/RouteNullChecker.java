package io.mycat.logTip;

import io.mycat.MycatExpection;

/**
 * @author jamie12221
 * @date 2019-05-06 00:38
 * 本类管理所有Router相关的日志或者异常提示
 **/
public enum RouteNullChecker {
  CHECK_MYCAT_SCHEMA_EXIST(0,"%s schema is not exist ");
  String format;
  int errorCode;

  RouteNullChecker(int errorCode,String format) {
    this.errorCode = errorCode;
    this.format = format;
  }


  public void check(String o,boolean contition) throws RuntimeException{
    if (!contition){
      throw new MycatExpection(errorCode,String.format(this.format, o));
    }
  }}
