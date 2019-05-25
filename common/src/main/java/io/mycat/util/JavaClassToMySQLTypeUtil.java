package io.mycat.util;

import io.mycat.beans.mysql.MySQLFieldsType;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * @author jamie12221
 *  date 2019-05-08 17:12
 **/
public class JavaClassToMySQLTypeUtil {
  public static int getMySQLType(Class<?> clazz){
    int actuallyType;
    if (clazz == Integer.class) {
      actuallyType = MySQLFieldsType.FIELD_TYPE_LONG;
    } else if (clazz == Long.class) {
      actuallyType = MySQLFieldsType.FIELD_TYPE_LONG;
    } else if (clazz == Byte.class) {
      actuallyType = MySQLFieldsType.FIELD_TYPE_TINY;
    } else if (clazz == Short.class) {
      actuallyType = MySQLFieldsType.FIELD_TYPE_SHORT;
    } else if (clazz == String.class) {
      actuallyType = MySQLFieldsType.FIELD_TYPE_STRING;
    } else if (clazz == Double.class) {
      actuallyType = MySQLFieldsType.FIELD_TYPE_DOUBLE;
    } else if (clazz == Float.class) {
      actuallyType = MySQLFieldsType.FIELD_TYPE_FLOAT;
    } else if (clazz == byte[].class) {
      actuallyType = MySQLFieldsType.FIELD_TYPE_BLOB;
    } else {
      throw new IllegalArgumentException("unsupport!");
    }
    return actuallyType;
  }
  public static byte[] getMySQLValue(Object object, Charset charset){
    Class<?> clazz = object.getClass();
    if (clazz == Long.class) {
      return LongUtil.toBytes((Long) object);
    } else if (clazz == Byte.class) {
      return  ByteUtil.getBytes((Byte)object);
    } else if (clazz == Short.class) {
      return  ByteUtil.getBytes((Short) object);
    } else if (clazz == String.class) {
      return  ((String) object).getBytes(charset);
    } else if (clazz == Double.class) {
      return ByteBuffer.allocate(8).putDouble((Double)object).array();
    } else if (clazz == Float.class) {
      return ByteBuffer.allocate(8).putDouble((Float)object).array();
    } else if (clazz == byte[].class) {
      return (byte[]) object;
    } else {
      throw new IllegalArgumentException("unsupport!");
    }
  }
}
