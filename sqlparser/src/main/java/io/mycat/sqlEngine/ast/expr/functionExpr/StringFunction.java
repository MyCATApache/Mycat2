package io.mycat.sqlEngine.ast.expr.functionExpr;

public class StringFunction {

  public static String concat(String... values) {
    if (values == null) {
      return null;
    }
    int len = 0;
    for (String val : values) {
      if (val == null) {
        return null;
      }
      len += val.length();
    }
    StringBuilder buf = new StringBuilder(len);
    for (String value : values) {
      buf.append(value);
    }
    return buf.toString();
  }

  public static String trim(String str) {
    if (str == null) {
      return str;
    }
    return str.trim();
  }

  public static String ucase(String str) {
    return upper(str);
  }

  public static String upper(String str) {
    if (str == null) {
      return str;
    }
    return str.toUpperCase();
  }
}
