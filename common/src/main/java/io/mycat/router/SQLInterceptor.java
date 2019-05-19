package io.mycat.router;

/**
 * used for interceptor sql before execute ,can modify sql befor execute
 *
 * @author wuzhih
 */
public interface SQLInterceptor {

  /**
   * return new sql to handler,ca't modify sql's type
   *
   * @return new sql
   */
  String interceptSQL(String sql);
}
